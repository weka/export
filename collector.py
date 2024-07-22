#
# collector module - implement prometheus_client collectors
#

# author: Vince Fleming, vince@weka.io


import datetime
import time
import traceback
from logging import getLogger
from threading import Lock

# local imports
import wekalib
from prometheus_client.core import GaugeMetricFamily, InfoMetricFamily, GaugeHistogramMetricFamily
from wekalib.circular import circular_list
from wekalib.wekatime import wekatime_to_datetime

from async_api import Async

#
# Module Globals
#

# initialize logger - configured in main routine
log = getLogger(__name__)


# makes life easier
def parse_sizes_values_post38(values):  # returns list of tuples of [(iosize,value),(iosize,value),...], and their sum
    # example input: [{'value': 2474458, 'start_range': 4096, 'end_range': 8192}]  - a list, NOT a string
    # log.debug(f"value_string={values}, type={type(values)}")
    gsum = 0
    stat_list = []

    for value in values:
        gsum += float(value['value']) / 60  # bug - have to divide by 60 secs to calc average per sec
        # stat_list.append( ( str(value['end_range']), float(value['value'])/60 ) )
        stat_list.append((str(value['end_range']), gsum))  # each bucket must be sum of all previous

    return stat_list, gsum


# makes life easier
def parse_sizes_values_pre38(
        value_string):  # returns list of tuples of [(iosize,value),(iosize,value),...], and their sum
    # example input: "[32768..65536] 19486, [65536..131072] 1.57837e+06"
    # log.debug(f"value_string={value_string}, type={type(value_string)}")
    gsum = 0
    stat_list = []
    values_list = value_string.split(", ")  # should be "[32768..65536] 19486","[65536..131072] 1.57837e+06"
    for values_str in values_list:  # value_list should be "[32768..65536] 19486" the first time through
        tmp = values_str.split("..")  # should be "[32768", "65536] 19486"
        tmp2 = tmp[1].split("] ")  # should be "65536","19486"
        gsum += float(tmp2[1])
        # stat_list.append( ( str(int(tmp2[0])-1), float(tmp2[1]) ) )
        stat_list.append((str(int(tmp2[0]) - 1), gsum))

    return stat_list, gsum


# our prometheus collector
class WekaCollector(object):
    WEKAINFO = {
        "hostList": dict(method="hosts_list", parms={}),
        "clusterinfo": dict(method="status", parms={}),
        "nodeList": dict(method="nodes_list", parms={}),
        "fs_stat": dict(method="filesystems_get_capacity", parms={}),
        "driveList": dict(method="disks_list", parms={"show_removed": False}),
        "alerts": dict(method="alerts_list", parms={}),
        "obs_capacity": dict(method="obs_capacities_list", parms={}),
    }
    CLUSTERSTATS = {
        'weka_overview_activity_ops': ['Weka IO Summary number of operations', ['cluster'], 'num_ops'],
        'weka_overview_activity_read_iops': ['Weka IO Summary number of read operations', ['cluster'], 'num_reads'],
        'weka_overview_activity_read_bytespersec': ['Weka IO Summary read rate', ['cluster'], 'sum_bytes_read'],
        'weka_overview_activity_write_iops': ['Weka IO Summary number of write operations', ['cluster'], 'num_writes'],
        'weka_overview_activity_write_bytespersec': ['Weka IO Summary write rate', ['cluster'], 'sum_bytes_written'],
        'weka_overview_activity_object_download_bytespersec': ['Weka IO Summary Object Download BPS', ['cluster'],
                                                               'obs_download_bytes_per_second'],
        'weka_overview_activity_object_upload_bytespersec': ['Weka IO Summary Object Upload BPS', ['cluster'],
                                                             'obs_upload_bytes_per_second']
    }

    INFOSTATS = {
        'weka_host_spares': ['Weka cluster # of hot spares', ["cluster"]],
        'weka_host_spares_bytes': ['Weka capacity of hot spares', ["cluster"]],
        'weka_drive_storage_total_bytes': ['Weka total drive capacity', ["cluster"]],
        'weka_drive_storage_unprovisioned_bytes': ['Weka unprovisioned drive capacity', ["cluster"]],
        'weka_num_servers_active': ['Number of active weka servers', ["cluster"]],
        'weka_num_servers_total': ['Total number of weka servers', ["cluster"]],
        'weka_num_clients_active': ['Number of active weka clients', ["cluster"]],
        'weka_num_clients_total': ['Total number of weka clients', ["cluster"]],
        'weka_num_drives_active': ['Number of active weka drives', ["cluster"]],
        'weka_num_drives_total': ['Total number of weka drives', ["cluster"]]
    }
    weka_stat_list = {}  # category: {{ stat:unit}, {stat:unit}}

    def __init__(self, config, cluster_obj):  # wekaCollector

        # dynamic module globals
        buckets = []  # same for everyone
        self._access_lock = Lock()
        self.gather_timestamp = None
        self.collect_time = None
        self.clusterdata = {}
        self.threaderror = False
        self.api_stats = {}
        exporter = config['exporter']
        self.max_procs = exporter['max_procs']
        self.max_threads_per_proc = exporter['max_threads_per_proc']
        self.backends_only = exporter['backends_only']
        if 'datapoints_per_collect' in exporter:
            self.datapoints_per_collect = exporter['datapoints_per_collect']
        else:
            self.datapoints_per_collect = 1
        self.map_registry = config["map_registry"]

        self.cluster = cluster_obj

        global weka_stat_list

        # print(json.dumps(config['stats'],indent=4))
        # weka_stat_list = config['stats']
        weka_stat_list = dict()
        one_call_stat = list()
        for category, stats in config['stats'].items():
            log.debug(f"category={category}, stats={stats}")
            if stats is not None:
                if category not in weka_stat_list:
                    log.debug(f"weka_stat_list={weka_stat_list}, stats={stats}")
                    weka_stat_list[category] = dict()
                for stat, unit in stats.items():
                    # log.debug(f"stat={stat}, unit={unit}")
                    weka_stat_list[category].update({stat: unit})
                    one_call_stat.append(f"{category}.{stat}")

        # log.debug(f"one_call_stat={len(one_call_stat)}")

        # log.debug(f"weka_stat_list={weka_stat_list}")

        # set up commands to get stats defined in config file
        # category: {{ stat:unit}, {stat:unit}}
        """
        for category, stat_dict in weka_stat_list.items():
            log.debug(f"category={category}, stat_dict={stat_dict}")
            if stat_dict is not None:
                #for stat, unit in stat_dict.items():
                #print(json.dumps(stat_dict))
                #for stat in stat_dict:
                # have to create the category keys, so do it with a try: block
                #if category not in self.wekaIOCommands.keys():
                #    #log.debug(f"Initializing category {category}")
                #    self.wekaIOCommands[category] = dict()

                parms = dict(category=[category] *len(stat_dict), stat=list(stat_dict.keys()), interval='1m', per_node=True, no_zeroes=True, show_internal=True)
                log.debug(parms)
                self.wekaIOCommands[category] = dict(method="stats_show", parms=parms)
        """

        #for stat in one_call_stat:
        #    log.debug(stat)
        parms = dict(stat=one_call_stat, interval=f'{self.datapoints_per_collect}m', per_node=True, no_zeroes=True,
                     show_internal=True)
        self.apicalls = dict(method="stats_show", parms=parms)

        # set up buckets, [4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, inf]
        for i in range(12, 12 + 16):
            buckets.append(2 ** i)

        buckets.append(float("inf"))

        log.debug("wekaCollector created")

    def get_weka_stat_list(self):
        return weka_stat_list

    # module global metrics allows for getting data from multiple clusters in multiple threads - DO THIS WITH A LOCK
    def _reset_metrics(self):
        # create all the metrics objects that we'll fill in elsewhere
        global metric_objs
        metric_objs = {'cmd_gather': GaugeMetricFamily('weka_gather_seconds', 'Time spent gathering statistics',
                                                       labels=["cluster"]),
                       'wekainfo': InfoMetricFamily('weka', "Information about the Weka cluster"),
                       'wekauptime': GaugeMetricFamily('weka_uptime', "Weka cluster uptime", labels=["cluster"])}
        for name, parms in self.CLUSTERSTATS.items():
            metric_objs["cluster_stat_" + name] = GaugeMetricFamily(name, parms[0], labels=parms[1])
        for name, parms in self.INFOSTATS.items():
            metric_objs[name] = GaugeMetricFamily(name, parms[0], labels=parms[1])
        metric_objs['weka_protection'] = GaugeMetricFamily('weka_protection', 'Weka Data Protection Status',
                                                           labels=["cluster", 'numFailures'])
        metric_objs['weka_rebuild'] = GaugeMetricFamily('weka_rebuild', 'Weka Rebuild Progress',
                                                        labels=["cluster"])
        metric_objs['weka_fs'] = GaugeMetricFamily('weka_fs', 'Filesystem information',
                                                   labels=['cluster', 'name', 'stat'])
        metric_objs['weka_stats_gauge'] = GaugeMetricFamily('weka_stats',
                                                            'WekaFS statistics. For more info refer to: https://docs.weka.io/usage/statistics/list-of-statistics',
                                                            labels=['cluster', 'host_name', 'host_role', 'node_id',
                                                                    'node_role', 'category', 'stat', 'unit'])
        metric_objs['weka_io_histogram'] = GaugeHistogramMetricFamily("weka_blocksize",
                                                                      "weka blocksize distribution histogram",
                                                                      labels=['cluster', 'host_name', 'host_role',
                                                                              'node_id', 'node_role', 'category',
                                                                              'stat', 'unit'])
        metric_objs['alerts'] = GaugeMetricFamily('weka_alerts', 'Alerts from Weka cluster',
                                                  labels=['cluster', 'type', 'title', 'host_name', 'host_id', 'node_id',
                                                          'drive_id'])
        metric_objs['drives'] = GaugeMetricFamily('weka_drives', 'Weka cluster drives',
                                                  labels=['cluster', 'host_name', 'host_id', 'node_id', 'drive_id',
                                                          'vendor', 'model', 'serial', 'size', 'status', 'life'])
        # what's the value?   Used?  Total? Over threshold?
        # metric_objs['obs_capacity'] = GaugeMetricFamily('weka_obj_capacity', 'Weka fs capacity',
        #                                          labels=['cluster', 'fs_name', 'fsId', 'bucket_name', 'reclaimable_percent',
        #                                                  'reclaimable_high_thresh', 'reclaimable_low_thresh',
        #                                                  'reclaimable_thresh', 'total_consumed_cap', 'used_cap'])
        metric_objs['fs_obs_total_consumed'] = GaugeMetricFamily('weka_fs_obj_total_consumed',
                                                                 'Weka fs capacity total consumed',
                                                                 labels=['cluster', 'fs_name', 'fsId', 'bucket_name'])
        metric_objs['fs_obs_cap_used'] = GaugeMetricFamily('weka_fs_obj_cap_used',
                                                           'Weka fs capacity used capacity',
                                                           labels=['cluster', 'fs_name', 'fsId', 'bucket_name'])
        metric_objs['fs_obs_cap_rec_percent'] = GaugeMetricFamily('weka_fs_obj_cap_rec_percent',
                                                                  'Weka fs capacity reclaimable capacity',
                                                                  labels=['cluster', 'fs_name', 'fsId', 'bucket_name'])
        metric_objs['fs_obs_cap_rec_thresh'] = GaugeMetricFamily('weka_fs_obj_cap_rec_thresh',
                                                                 'Weka fs capacity reclaimable threshold',
                                                                 labels=['cluster', 'fs_name', 'fsId', 'bucket_name'])
        metric_objs['fs_obs_cap_rec_low_thresh'] = GaugeMetricFamily('weka_fs_obj_cap_rec_low_thresh',
                                                                     'Weka fs capacity reclaimable low threshold',
                                                                     labels=['cluster', 'fs_name', 'fsId',
                                                                             'bucket_name'])
        metric_objs['fs_obs_cap_rec_hi_thresh'] = GaugeMetricFamily('weka_fs_obj_cap_rec_hi_thresh',
                                                                    'Weka fs capacity reclaimable high threshold',
                                                                    labels=['cluster', 'fs_name', 'fsId',
                                                                            'bucket_name'])

    def collect(self):

        # lock_starttime = time.time()
        with self._access_lock:  # be thread-safe, and don't conflict with events collection
            # log.info(f"Waited {round(time.time() - lock_starttime, 2)}s to obtain lock")

            self.api_stats['num_calls'] = 0

            log.debug("Entering collect() routine")
            second_pass = False
            should_gather = False

            # get the time once here
            start_time = time.time()

            # first time being called?   force gathering info?
            if self.collect_time is None:
                log.debug("never gathered before")
                should_gather = True
            elif start_time - self.collect_time > 5:  # prometheus always calls twice; only gather if it's been a while since last call
                should_gather = True
            else:
                second_pass = True

            if should_gather:
                log.info("gathering")
                try:
                    self.gather()
                except wekalib.exceptions.NameNotResolvable as exc:
                    log.critical(f"Unable to resolve names")
                    labelvalues = [str(self.cluster), 'ExporterResolveError',
                                   f'weka-mon exporter cannot collect data: {exc}',
                                   "None", "None", "None", "None"]
                    metric_objs['alerts'].add_metric(labelvalues, 1.0)
                except Exception as exc:
                    log.critical(f"Error gathering data: {exc}, {traceback.format_exc()}")
                    labelvalues = [str(self.cluster), 'ExporterCriticalError',
                                   f'weka-mon exporter cannot collect data: {exc}',
                                   "None", "None", "None", "None"]
                    metric_objs['alerts'].add_metric(labelvalues, 1.0)

            # yield for each metric 
            log.debug("Yielding metrics")
            for metric in metric_objs.values():
                yield metric
            log.debug("Yielding complete")

            # report time if we gathered, otherwise, it's meaningless
            if should_gather:
                self.last_elapsed = time.time() - start_time
            else:
                elapsed = self.last_elapsed

            log.debug("Yielding process metrics")
            yield GaugeMetricFamily('weka_collect_seconds', 'Total Time spent in Prometheus collect',
                                    value=self.last_elapsed)
            yield GaugeMetricFamily('weka_collect_apicalls', 'Total number of api calls',
                                    value=self.api_stats['num_calls'])
            log.debug("Yielding process metrics complete")

            if not second_pass:
                log.info(
                    f"stats returned. total time = {round(self.last_elapsed, 2)}s {self.api_stats['num_calls']} api calls made. {time.asctime()}")
            self.collect_time = time.time()

    # runs in a thread, so args comes in as a dict
    def call_api(self, metric, category, args):

        cluster = self.cluster
        self.api_stats['num_calls'] += 1
        method = args['method']
        parms = args['parms']
        log.debug(f"method={method}, parms={parms}")
        try:
            if category is None:
                self.clusterdata[str(cluster)][metric] = cluster.call_api(method=method, parms=parms)
            else:
                if category not in self.clusterdata[str(cluster)]:
                    self.clusterdata[str(cluster)][category] = {}
                api_return = cluster.call_api(method=method, parms=parms)
                if metric not in self.clusterdata[str(cluster)][category]:
                    log.debug(f"first call for {category}/{metric}")
                    self.clusterdata[str(cluster)][category][metric] = api_return
                else:
                    log.debug(f"follow-on call for {category}/{metric}")
                    self.clusterdata[str(cluster)][category][metric] += api_return
                    # then we already have data for this category - must be a lot of nodes (>100)
        except Exception as exc:
            # just log it, as we're probably in a thread
            log.critical(f"Exception caught: {exc}, {traceback.format_exc()}")
            # log.debug(traceback.format_exc())

    def store_results(self, cluster, results):
        for result in results:  # remember, APICall objects
            stat = result.opaque[0]
            category = result.opaque[1]

            if result.status == "good":
                self.api_stats['num_calls'] += 1
                if category is not None:
                    if category not in self.clusterdata[str(cluster)]:
                        self.clusterdata[str(cluster)][category] = dict()
                    if stat not in self.clusterdata[str(cluster)][category]:
                        self.clusterdata[str(cluster)][category][stat] = result.result
                    else:
                        self.clusterdata[str(cluster)][category][stat] += result.result
                else:
                    if stat not in self.clusterdata[str(cluster)]:
                        self.clusterdata[str(cluster)][stat] = result.result
                    else:
                        self.clusterdata[str(cluster)][stat] += result.result

    #
    # gather() gets fresh stats from the cluster as they update
    #       populates all datastructures with fresh data
    #
    #
    def gather(self):

        cluster = self.cluster
        self._reset_metrics()

        start_time = time.time()
        log.info("gathering weka data from cluster {}".format(str(cluster)))

        # re-initialize wekadata so changes in the cluster don't leave behind strange things (hosts/nodes that no longer exist, etc)
        wekadata = {}
        self.clusterdata[str(cluster)] = wekadata  # clear out old data

        # reset the cluster config to be sure we can talk to all the hosts
        try:
            cluster.refresh()
        except wekalib.exceptions.NameNotResolvable as exc:
            log.critical(f"Names are not resolvable - are they in /etc/hosts or DNS? {exc}")
            raise
        except Exception as exc:
            log.error(f"Cluster refresh failed on cluster '{cluster}' - check connectivity ({exc})")
            raise

        # set up async api calling subsystem
        self.asyncobj = Async(cluster, self.max_procs, self.max_threads_per_proc)

        # get info from weka cluster - these are quick calls
        for stat, command in self.WEKAINFO.items():
            try:
                wekadata[stat] = cluster.call_api(command['method'], command['parms'])
                self.api_stats['num_calls'] += 1
            except Exception as exc:
                log.error(f"error getting {stat} from cluster {cluster}: {exc}")
                # decision - if we can't get the basic info, we can't get anything else, so abort?

        # clear old maps, if any - if nodes come/go this can get funky with old data, so re-create it every time
        node_host_map = dict()
        node_role_map = dict()
        host_role_map = dict()

        # populate maps
        try:
            for node_id, node in wekadata["nodeList"].items():
                node_host_map[node_id] = node["hostname"]
                node_role_map[node_id] = node["roles"]
                # node["node_id"] = node_id   # this used to be inside here...
            for host in wekadata["hostList"].values():  # node is a dict of node attribute
                if host['hostname'] not in host_role_map:  # there may be MCB, so might be there already
                    if host["mode"] == "backend":
                        host_role_map[host["hostname"]] = "server"
                    else:
                        host_role_map[host["hostname"]] = "client"
            # update the maps so they can be used in the loki module
            self.map_registry.register('node-host', node_host_map)
            self.map_registry.register('node-role', node_role_map)
            self.map_registry.register('node-role', host_role_map)
        except Exception as exc:
            log.error(f"error building maps: {exc}: Aborting data gather from cluster {str(cluster)}")
            raise

        log.info(f"Cluster {cluster} Using {cluster.sizeof()} hosts")

        # be simplistic at first... let's just gather on a subset of nodes each query
        # all_nodes = backend_nodes + client_nodes    # concat both lists

        #
        # new tactic - use all hosts for their own stats... node_maps is a dict of roles, each with a dict of host:[nids]
        #
        #

        node_maps = {"FRONTEND": {}, "COMPUTE": {}, "DRIVES": {}, "MANAGEMENT": {}}  # initial state of maps

        # log.debug(f'{weka_maps["node-role"]}')

        for node in node_role_map:  # node == "NodeId<xx>"
            for role in node_role_map[node]:
                nid = int(node.split('<')[1].split('>')[0])  # make nodeid numeric
                hostname = node_host_map[node]
                if hostname not in node_maps[role]:
                    node_maps[role][hostname] = list()
                node_maps[role][hostname].append(nid)  # needs to be dict of host:[nid]

        # log.debug(f"{cluster.name} {json.dumps(node_maps, indent=4)}")

        # find a better place to define this... for now here is good (vince)
        category_nodetypes = {
            'cpu': ['FRONTEND', 'COMPUTE', 'DRIVES'],
            'ops': ['FRONTEND'],
            'ops_driver': ['FRONTEND'],
            'ops_nfs': ['COMPUTE'],  # not sure about this one
            'ssd': ['DRIVES'],
            'network': ['FRONTEND', 'COMPUTE', 'DRIVES']
        }

        # new impl
        # up_list is a list of all the good hosts (ie: not down)
        up_list = list()
        backends_list = list()
        for host in wekadata['hostList'].values():
            if host['status'] == 'UP' and host['state'] == 'ACTIVE' and host['hostname'] not in up_list:
                up_list.append(host['hostname'])
                if host['mode'] == 'backend':
                    backends_list.append(host['hostname'])

        # log.debug(f"node_host_map ={node_host_map}")
        one_call_nids = dict()
        for node, hostname in node_host_map.items():
            nid = int(node.split('<')[1].split('>')[0])  # make nodeid numeric
            if hostname not in one_call_nids:
                one_call_nids[hostname] = [nid]
            else:
                one_call_nids[hostname].append(nid)

        # for host, node in weka_maps["node-host"]:
        # log.debug(f"one_call_nids={json.dumps(one_call_nids, indent=2)}")

        #for stat in self.apicalls['parms']['stat']:
        #    log.debug(stat)

        circular_host_list = circular_list(inputlist=backends_list)

        for hostname, nids in one_call_nids.items():
            import copy
            # don't query for down hosts
            if hostname in up_list:
                newcmd = copy.deepcopy(self.apicalls)  # make sure to copy it
                newcmd["parms"]["node_ids"] = copy.deepcopy(nids)
                # set up newcmd
                # vince - not sure this is working as intended
                target = circular_host_list.next() if self.backends_only else hostname
                self.asyncobj.submit(target, newcmd['method'], newcmd['parms'])
                self.api_stats['num_calls'] += 1

        log.debug("******************************** WAITING ON ASYNC PROCESS *************************************")
        stats_data = list()
        for result in self.asyncobj.wait():
            if not result.exception:
                # log.info(f"result={result}")
                stats_data.append(result.result)
            else:
                log.error(f'result has exception {result.exception}')
        log.debug(
            "******************************** WAITING ON ASYNC PROCESS COMPLETE *************************************")

        elapsed = time.time() - start_time
        log.debug(f"gather for cluster {cluster} complete.  Elapsed time {elapsed}")
        metric_objs['cmd_gather'].add_metric([str(cluster)], value=elapsed)

        # if the cluster changed during a gather, this may puke, so just go to the next sample.
        #   One or two missing samples won't hurt

        #  Start filling in the data
        log.info("populating datastructures for cluster {}".format(str(cluster)))
        try:
            # determine Cloud Status 
            if wekadata["clusterinfo"]["cloud"]["healthy"]:
                cloudStatus = "Healthy"  # must be enabled to be healthy
            elif wekadata["clusterinfo"]["cloud"]["enabled"]:
                cloudStatus = "Unhealthy"  # enabled, but unhealthy
            else:
                cloudStatus = "Disabled"  # disabled, healthy is meaningless
        except:
            log.error("error processing cloud status for cluster {}".format(str(cluster)))

        #
        # start putting the data into the prometheus_client gauges and such
        #

        # set the weka_info Gauge
        log.debug(f"weka_info Gauge cluster={cluster.name}")
        try:
            # Weka status indicator
            if (wekadata["clusterinfo"]["buckets"]["active"] == wekadata["clusterinfo"]["buckets"]["total"] and
                    wekadata["clusterinfo"]["drives"]["active"] == wekadata["clusterinfo"]["drives"]["total"] and
                    wekadata["clusterinfo"]["io_nodes"]["active"] == wekadata["clusterinfo"]["io_nodes"]["total"] and
                    wekadata["clusterinfo"]["hosts"]["backends"]["active"] ==
                    wekadata["clusterinfo"]["hosts"]["backends"]["total"]):
                WekaClusterStatus = "OK"
            else:
                WekaClusterStatus = "WARN"

            # Basic info
            cluster.release = wekadata["clusterinfo"]["release"]  # keep this up to date
            wekacluster = {"cluster": str(cluster), "version": wekadata["clusterinfo"]["release"],
                           "cloud_status": cloudStatus, "license_status": wekadata["clusterinfo"]["licensing"]["mode"],
                           "io_status": wekadata["clusterinfo"]["io_status"],
                           "link_layer": wekadata["clusterinfo"]["net"]["link_layer"], "status": WekaClusterStatus}

            metric_objs['wekainfo'].add_metric(labels=wekacluster.keys(), value=wekacluster)

            # log.info( "cluster name: " + wekadata["clusterinfo"]["name"] )
        except Exception as exc:
            log.error("error cluster info - aborting populate of cluster {}".format(str(cluster)))
            raise

        log.debug(f"uptime cluster={cluster.name}")
        try:
            # Uptime
            # not sure why, but sometimes this would fail... trim off the microseconds, because we really don't care 
            cluster_time = self._trim_time(wekadata["clusterinfo"]["time"]["cluster_time"])
            cluster_start_time = self._trim_time(wekadata["clusterinfo"]["io_status_changed_time"])
            now_obj = datetime.datetime.strptime(cluster_time, "%Y-%m-%dT%H:%M:%S")
            dt_obj = datetime.datetime.strptime(cluster_start_time, "%Y-%m-%dT%H:%M:%S")
            uptime = now_obj - dt_obj
            metric_objs["wekauptime"].add_metric([str(cluster)], uptime.total_seconds())
        except:
            # track = traceback.format_exc()
            # print(track)
            log.error("error calculating runtime for cluster {}".format(str(cluster)))

        log.debug(f"perf overview cluster={cluster.name}")
        try:
            # performance overview summary
            # I suppose we could change the gauge names to match the keys, ie: "num_ops" so we could do this in a loop
            #       e: weka_overview_activity_num_ops instead of weka_overview_activity_ops
            for name, parms in self.CLUSTERSTATS.items():
                metric_objs["cluster_stat_" + name].add_metric([str(cluster)],
                                                               wekadata["clusterinfo"]["activity"][parms[2]])

        except:
            # track = traceback.format_exc()
            # print(track)
            log.error("error processing performance overview for cluster {}".format(str(cluster)))

        log.debug(f"server overview cluster={cluster.name}")
        try:
            metric_objs['weka_host_spares'].add_metric([str(cluster)], wekadata["clusterinfo"]["hot_spare"])
            metric_objs['weka_host_spares_bytes'].add_metric([str(cluster)],
                                                             wekadata["clusterinfo"]["capacity"]["hot_spare_bytes"])
            metric_objs['weka_drive_storage_total_bytes'].add_metric([str(cluster)],
                                                                     wekadata["clusterinfo"]["capacity"]["total_bytes"])
            metric_objs['weka_drive_storage_unprovisioned_bytes'].add_metric([str(cluster)],
                                                                             wekadata["clusterinfo"]["capacity"][
                                                                                 "unprovisioned_bytes"])
            metric_objs['weka_num_servers_active'].add_metric([str(cluster)],
                                                              wekadata["clusterinfo"]["hosts"]["backends"]["active"])
            metric_objs['weka_num_servers_total'].add_metric([str(cluster)],
                                                             wekadata["clusterinfo"]["hosts"]["backends"]["total"])
            metric_objs['weka_num_clients_active'].add_metric([str(cluster)],
                                                              wekadata["clusterinfo"]["hosts"]["clients"]["active"])
            metric_objs['weka_num_clients_total'].add_metric([str(cluster)],
                                                             wekadata["clusterinfo"]["hosts"]["clients"]["total"])
            metric_objs['weka_num_drives_active'].add_metric([str(cluster)],
                                                             wekadata["clusterinfo"]["drives"]["active"])
            metric_objs['weka_num_drives_total'].add_metric([str(cluster)], wekadata["clusterinfo"]["drives"]["total"])
        except:
            log.error("error processing server overview for cluster {}".format(str(cluster)))

        log.debug(f"protection status cluster={cluster.name}")
        try:
            # protection status
            rebuildStatus = wekadata["clusterinfo"]["rebuild"]
            protectionStateList = rebuildStatus["protectionState"]
            numStates = len(protectionStateList)  # 3 (0,1,2) for 2 parity), or 5 (0,1,2,3,4 for 4 parity)

            for index in range(numStates):
                metric_objs['weka_protection'].add_metric(
                    [str(cluster), str(protectionStateList[index]["numFailures"])],
                    protectionStateList[index]["percent"])

            metric_objs['weka_rebuild'].add_metric([str(cluster)], rebuildStatus["progressPercent"])


        except:
            log.error("error processing protection status for cluster {}".format(str(cluster)))

        log.debug(f"filesystems cluster={cluster.name}")
        try:
            # Filesystem stats
            for fs_id, fs in wekadata["fs_stat"].items():
                fs['total_percent_used'] = float(fs["used_total"]) / float(fs["available_total"]) * 100
                fs['ssd_percent_used'] = float(fs["used_ssd"]) / float(fs["available_ssd"]) * 100

                for fs_stat in ['available_total', 'used_total', 'available_ssd', 'used_ssd', 'total_percent_used',
                                'ssd_percent_used']:
                    metric_objs['weka_fs'].add_metric([str(cluster), fs["name"], fs_stat], fs[fs_stat])

        except:
            log.error("error processing filesystem stats for cluster {}".format(str(cluster)))

        log.debug(f"alerts cluster={cluster.name}")
        try:
            for alert in wekadata["alerts"]:
                if not alert["muted"]:
                    log.debug(f"alert detected {alert['type']}")
                    host_name = "None"
                    host_id = "None"
                    node_id = "None"
                    drive_id = "None"
                    if "params" in alert:
                        # print( json.dumps(alert["params"], indent=4, sort_keys=True) )
                        params = alert['params']
                        if 'hostname' in params:
                            host_name = params['hostname']
                        if 'host_id' in params:
                            host_id = params['host_id']
                        if 'node_id' in params:
                            node_id = params['node_id']
                        if 'drive_id' in params:
                            drive_id = params['drive_id']
                    labelvalues = [str(cluster), alert['type'], alert['title'], host_name, host_id, node_id, drive_id]
                    metric_objs['alerts'].add_metric(labelvalues, 1.0)
        except Exception as exc:
            log.error(f"error {exc} processing alerts for cluster {str(cluster)}")

        try:
            for disk_id, drive in wekadata["driveList"].items():
                # removed drives can have null values - prom client code hates that!
                if drive['hostname'] is None or len(drive['hostname']) == 0:
                    drive['hostname'] = "None"

                if drive['vendor'] is None:
                    drive['vendor'] = "Unknown"
                if drive['model'] is None:
                    drive['model'] = "Unknown"
                if drive['serial_number'] is None:
                    drive['serial_number'] = "Unknown"

                metric_objs['drives'].add_metric(
                    [
                        str(cluster),
                        drive['hostname'],
                        drive['host_id'],
                        drive['node_id'],
                        disk_id,
                        drive['vendor'],
                        drive['model'],
                        drive['serial_number'],
                        str(drive['size_bytes']),
                        drive['status'],
                        str(100 - int(drive['percentage_used']))
                    ], 1)

        except:
            log.error(f"error processing DRIVES for cluster {cluster}")

        # metric_objs['obs_capacity'] = GaugeMetricFamily('weka_obj_capacity', 'Weka fs capacity',
        #                                          labels=['cluster', 'fs_name', 'fsId', 'bucket_name', 'reclaimable_percent',
        #                                                  'reclaimable_high_thresh', 'reclaimable_low_thresh',
        #                                                  'reclaimable_thresh', 'total_consumed_cap', 'used_cap'])
        try:
            for fs in wekadata["obs_capacity"]:
                metric_objs['fs_obs_total_consumed'].add_metric([str(cluster), fs['filesystem_name'], fs['fsId'],
                                                                 fs['obs_bucket_name']], fs['total_consumed_capacity'])
                metric_objs['fs_obs_cap_used'].add_metric([str(cluster), fs['filesystem_name'], fs['fsId'],
                                                           fs['obs_bucket_name']], fs['used_capacity'])
                metric_objs['fs_obs_cap_rec_percent'].add_metric([str(cluster), fs['filesystem_name'], fs['fsId'],
                                                                  fs['obs_bucket_name']], fs['reclaimable'])
                metric_objs['fs_obs_cap_rec_thresh'].add_metric([str(cluster), fs['filesystem_name'], fs['fsId'],
                                                                 fs['obs_bucket_name']], fs['reclaimable_threshold'])
                metric_objs['fs_obs_cap_rec_low_thresh'].add_metric([str(cluster), fs['filesystem_name'], fs['fsId'],
                                                                     fs['obs_bucket_name']],
                                                                    fs['reclaimable_low_threshold'])
                metric_objs['fs_obs_cap_rec_hi_thresh'].add_metric([str(cluster), fs['filesystem_name'], fs['fsId'],
                                                                    fs['obs_bucket_name']],
                                                                   fs['reclaimable_high_threshold'])
        except Exception as exc:
            log.error(f"error processing obs_capacity for cluster {cluster}:{exc}")

        """
        [{
          "category": "ops",
          "node": "NodeId<1>",
          "stat_type": "READ_BYTES",
          "stat_value": 4189934.933333333,
          "timestamp": "2021-08-16T15:23:00Z"
        },
          {
          "category": "ops",
          "node": "NodeId<1>",
          "stat_type": "THROUGHPUT",
          "stat_value": 4189934.933333333,
          "timestamp": "2021-08-16T15:23:00Z"
        }]
        """

        def get_stat(raw_stats_data):
            # New WEKA stats data a bit convoluted, so we'll use this to make it easier to understand
            # this routine returns a single stat at a time, with all it's relevant information
            for raw_resp in raw_stats_data:
                for _node, cat_dict in raw_resp.items():  # "NodeId<41>": {"ops": [{ "stats": { "OPS": 97.33333333333333,
                    for _category, info_list in cat_dict.items():
                        for _item in info_list:
                            for _stat, _value in _item['stats'].items():
                                _timestamp = _item['timestamp']
                                try:
                                    _hostname = node_host_map[_node]
                                    _host_role = host_role_map[_hostname]
                                    _role_list = node_role_map[_node]
                                    _unit = weka_stat_list[_category][_stat]
                                    if len(_role_list) > 1:
                                        _role = "multiple"  # punt for now? Vince  - Might want to list CPU_UTIL multiple times, once per role??
                                    else:
                                        _role = _role_list[0]
                                except Exception as exc:
                                    log.error(f"{exc} error in maps for cluster {str(cluster)}")
                                    continue  # ignore the error
                                    # log.error(traceback.format_exc())

                                if type(_value) == int:
                                    _value = float(_value)  # some WEKA stats are int or histograms

                                yield _hostname, _host_role, _node, _role, _category, _stat, _value, _timestamp, _unit

        # format and submit the stats to the prometheus client
        for hostname, host_role, node, role, category, stat, value, timestamp, unit in get_stat(stats_data):
            label_values = [
                str(cluster),
                hostname,
                host_role,
                node,
                role,
                category,
                stat,
                unit]
            # log.debug(f"unit={unit}")
            if type(value) == float:
                try:
                    metric_objs['weka_stats_gauge'].add_metric(label_values, value,
                                                               timestamp=wekatime_to_datetime(timestamp).timestamp())
                except Exception as exc:
                    print(f"{traceback.format_exc()}")
                    log.error(f"error processing io stats for cluster {cluster}:{exc}")
            else:
                # this is a histogram (I hope...)
                try:
                    value_dict, gsum = parse_sizes_values_post38(value)  # Turn the stat_value into a dict
                    # log.debug(f"{value_dict}, {gsum}")
                    metric_objs['weka_io_histogram'].add_metric(labels=label_values, buckets=value_dict,
                                                                gsum_value=gsum)
                except Exception as exc:
                    log.error(f"{traceback.format_exc()}")
                    log.error(f"error processing Histogram stat for cluster {cluster}:{exc}")

        # shut down the child processes
        log.debug(f"shutting down children")
        del self.asyncobj

        log.info(f"Gather complete: cluster={cluster}, total elapsed={str(round(time.time() - start_time, 2))}")

    # ------------- end of gather() -------------

    def collect_logs(self, lokiserver):
        with self._access_lock:  # make sure we don't conflict with a metrics collection
            log.info(f"getting events for cluster {self.cluster}")
            try:
                events = self.cluster.setup_events()  # this uses cluster api, so needs a lock
            except Exception as exc:
                log.critical(f"Error setting up for events: {exc} for cluster {self.cluster}")
                # log.critical(f"{traceback.format_exc()}")
                return

        try:
            events = self.cluster.get_events()  # this goes to weka home, so no lock needed
        except Exception as exc:
            log.critical(f"Error getting events: {exc} for cluster {self.cluster}")
            # log.critical(f"{traceback.format_exc()}")
            return

        try:
            lokiserver.send_events(events, self.cluster)
        except Exception as exc:
            log.critical(f"Error sending events: {exc} for cluster {self.cluster}")
            # log.critical(f"{traceback.format_exc()}")

    @staticmethod
    def _trim_time(time_string):
        tmp = time_string.split('.')
        return tmp[0]
