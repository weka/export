#
# collector module - implement prometheus_client collectors
#

# author: Vince Fleming, vince@weka.io


from prometheus_client import Gauge, Histogram
from prometheus_client.core import GaugeMetricFamily, InfoMetricFamily, GaugeHistogramMetricFamily
import time
import datetime
from threading import Lock
import traceback
import logging
import logging.handlers
from logging import debug, info, warning, error, critical, getLogger, DEBUG
import json
import sys
from async import Async

# local imports
from wekalib.wekaapi import WekaApi
from wekalib.wekatime import wekatime_to_datetime
import wekalib


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
        "alerts": dict(method="alerts_list", parms={})
        # "quotas": dict( method="directory_quota_list", parms={'fs_name','start_cookie'} )
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
    wekaIOCommands = {}
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
        self.max_procs = config['exporter']['max_procs']
        self.max_threads_per_proc = config['exporter']['max_threads_per_proc']

        self.cluster = cluster_obj

        global weka_stat_list

        weka_stat_list = config['stats']

        #log.debug(f"weka_stat_list={weka_stat_list}")

        # set up commands to get stats defined in config file
        # category: {{ stat:unit}, {stat:unit}}
        for category, stat_dict in weka_stat_list.items():
            #log.debug(f"category={category}, stat_dict={stat_dict}")
            if stat_dict is not None:
                for stat, unit in stat_dict.items():
                    # have to create the category keys, so do it with a try: block
                    if category not in self.wekaIOCommands.keys():
                        #log.debug(f"Initializing category {category}")
                        self.wekaIOCommands[category] = {}

                    parms = dict(category=category, stat=stat, interval='1m', per_node=True, no_zeroes=True)
                    self.wekaIOCommands[category][stat] = dict(method="stats_show", parms=parms)

        # set up buckets, [4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, inf]
        for i in range(12, 12 + 16):
            buckets.append(2 ** i)

        buckets.append(float("inf"))

        log.debug("wekaCollector created")

    def get_commandlist(self):
        return self.wekaIOCommands

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
        metric_objs['weka_fs'] = GaugeMetricFamily('weka_fs', 'Filesystem information', labels=['cluster', 'name', 'stat'])
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

    def collect(self):

        #lock_starttime = time.time()
        with self._access_lock:  # be thread-safe, and don't conflict with events collection
            #log.info(f"Waited {round(time.time() - lock_starttime, 2)}s to obtain lock")

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
            elif start_time - self.collect_time > 5:    # prometheus always calls twice; only gather if it's been a while since last call
                should_gather = True
            else:
                second_pass = True


            if should_gather:
                log.info("gathering")
                try:
                    self.gather()
                except wekalib.exceptions.NameNotResolvable as exc:
                    log.critical(f"Unable to resolve names; terminating")
                    sys.exit(1)
                except Exception as exc:
                    log.critical(f"Error gathering data: {exc}, {traceback.format_exc()}")
                    #log.critical(traceback.format_exc())
                    return  # raise?

            # yield for each metric 
            for metric in metric_objs.values():
                yield metric

            # report time if we gathered, otherwise, it's meaningless
            if should_gather:
                self.last_elapsed = time.time() - start_time
            else:
                elapsed = self.last_elapsed

            yield GaugeMetricFamily('weka_collect_seconds', 'Total Time spent in Prometheus collect', value=self.last_elapsed)
            yield GaugeMetricFamily('weka_collect_apicalls', 'Total number of api calls',
                                    value=self.api_stats['num_calls'])

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
            #log.debug(traceback.format_exc())


    def store_results(self, cluster, results):
        # new stuff - vince
        for result in results:      # remember, APICall objects
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
                        self.clusterdata[str(cluster)][stat] =  result.result
                    else:
                        self.clusterdata[str(cluster)][stat] +=  result.result


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
            #log.error(traceback.format_exc())
            return

        # set up async api calling subsystem
        self.async = Async(cluster, self.max_procs, self.max_threads_per_proc)

        # get info from weka cluster - these are quick calls
        for stat, command in self.WEKAINFO.items():
            wekadata[stat] = cluster.call_api(command['method'], command['parms'])
            self.api_stats['num_calls'] += 1

        #log.debug("******************************** WAITING ON ASYNC PROCESS *************************************")
        #results = cluster.wait_async()    # wait for api calls to complete (returns APICall objects)
        #log.debug("******************************** ASYNC PROCESS COMPLETE ***************************************")

        #if len(results) == 0:
        #    log.critical(f"api unable to contact cluster {cluster}; aborting gather")
        #    return

        # move results into the wekadata structure
        #self.store_results(cluster, results)


        # build maps - need this for decoding data, not collecting it.
        #    do in a try/except block because it can fail if the cluster changes while we're collecting data

        # clear old maps, if any - if nodes come/go this can get funky with old data, so re-create it every time
        weka_maps = {"node-host": {}, "node-role": {}, "host-role": {}}  # initial state of maps

        # populate maps
        try:
            for node in wekadata["nodeList"]:
                weka_maps["node-host"][node["node_id"]] = node["hostname"]
                weka_maps["node-role"][node["node_id"]] = node["roles"]  # note - this is a list
            for host in wekadata["hostList"]:
                if host["mode"] == "backend":
                    weka_maps["host-role"][host["hostname"]] = "server"
                else:
                    weka_maps["host-role"][host["hostname"]] = "client"
        except Exception as exc:
            log.error("error building maps. Aborting data gather from cluster {}".format(str(cluster)))
            return

        log.info(f"Cluster {cluster} Using {cluster.sizeof()} hosts")

        # be simplistic at first... let's just gather on a subset of nodes each query
        # all_nodes = backend_nodes + client_nodes    # concat both lists

        #
        # new tactic - use all hosts for their own stats... node_maps is a dict of roles, each with a dict of host:[nids]
        #
        #

        #node_maps = {"FRONTEND": [], "COMPUTE": [], "DRIVES": [], "MANAGEMENT": []}  # initial state of maps
        node_maps = {"FRONTEND": {}, "COMPUTE": {}, "DRIVES": {}, "MANAGEMENT": {}}  # initial state of maps

        # log.debug(f'{weka_maps["node-role"]}')

        for node in weka_maps["node-role"]:  # node == "NodeId<xx>"
            for role in weka_maps['node-role'][node]:
                nid = int(node.split('<')[1].split('>')[0])  # make nodeid numeric
                hostname = weka_maps["node-host"][node]
                if hostname not in node_maps[role]:
                    node_maps[role][hostname] = list()
                node_maps[role][hostname].append(nid) # needs to be dict of host:[nid]

        #log.debug(f"{cluster.name} {json.dumps(node_maps, indent=4)}")

        # find a better place to define this... for now here is good (vince)
        category_nodetypes = {
            'cpu': ['FRONTEND', 'COMPUTE', 'DRIVES'],
            'ops': ['FRONTEND'],
            'ops_driver': ['FRONTEND'],
            'ops_nfs': ['COMPUTE'],  # not sure about this one
            'ssd': ['DRIVES'],
            'network': ['FRONTEND', 'COMPUTE', 'DRIVES']
        }

        # schedule a bunch of data gather queries
        for category, stat_dict in self.get_commandlist().items():

            category_nodes = {}     # dict now... host:[nid]
            # log.error(f"{cluster.name} category is: {category} {category_nodetypes[category]}")
            for nodetype in category_nodetypes[category]:  # nodetype is FRONTEND, COMPUTE, DRIVES, MANAGEMENT
                log.debug(f"nodetype={nodetype}, node_maps[nodetype] = {node_maps[nodetype]}")
                for host, nodes in node_maps[nodetype].items():
                    if host in category_nodes.keys():
                        category_nodes[host] = list(set().union(category_nodes[host],nodes))   # merge nodes lists uniquely
                    else:
                        category_nodes[host] = nodes

            #log.debug(f"{cluster.name} cat nodes: {category} {json.dumps(category_nodes, indent=4)}")  # debugging

            # not sure what to do here... think...
            #query_nodes = list(
            #    set(category_nodes.copy()))  # make the list unique so we don't ask for the same data muliple times

            log.debug(f"category={category}, stat_dict={stat_dict}")
            for stat, command in stat_dict.items():
                for hostname, nids in category_nodes.items():    # hostinfo_dict is host:[nid]
                    import copy
                    newcmd = copy.deepcopy(command)  # make sure to copy it
                    newcmd["parms"]["node_ids"] = copy.deepcopy(nids)  # make sure to copy it

                    #hostobj = cluster.get_hostobj_byname(hostname)
                    #log.debug(f"scheduling {hostname} {newcmd['parms']}")  # debugging

                    # schedule more asychronous api calls...
                    try:
                        #hostobj.async_call_api((stat, category), newcmd['method'], newcmd['parms'])   # host.async_call_api instead?
                        self.async.submit(hostname, category, stat, newcmd['method'], newcmd['parms'])
                        self.api_stats['num_calls'] += 1
                    except:
                        log.error("gather(): error scheduling thread wekastat for cluster {}".format(str(cluster)))
                        print(traceback.format_exc())
                        sys.exit(1)


        log.debug("******************************** WAITING ON ASYNC PROCESS *************************************")
        #results = cluster.wait_async()    # wait for api calls to complete (returns APICall objects)
        #for hostname in cluster.hosts():
        #    hostobj = cluster.get_hostobj_byname(hostname)
        #    results = hostobj.wait_async()    # wait for api calls to complete (returns APICall objects)
        #    # move results into the wekadata structure
        #    self.store_results(cluster, results)

        for result in self.async.wait():
            if not result.exception:
                if result.category not in wekadata:
                    wekadata[result.category] = dict()
                if result.stat not in wekadata[result.category]:
                    wekadata[result.category][result.stat] = list()
                #log.debug(f"result = {result.result}")
                wekadata[result.category][result.stat] += result.result
        log.debug("******************************** WAITING ON ASYNC PROCESS COMPLETE *************************************")

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
        except:
            log.error("error cluster info - aborting populate of cluster {}".format(str(cluster)))
            return

        log.debug(f"uptime cluster={cluster.name}")
        try:
            # Uptime
            # not sure why, but sometimes this would fail... trim off the microseconds, because we really don't care 
            cluster_time = self._trim_time(wekadata["clusterinfo"]["time"]["cluster_time"])
            start_time = self._trim_time(wekadata["clusterinfo"]["io_status_changed_time"])
            now_obj = datetime.datetime.strptime(cluster_time, "%Y-%m-%dT%H:%M:%S")
            dt_obj = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
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
            for fs in wekadata["fs_stat"]:
                fs['total_percent_used'] = float(fs["used_total"]) / float(fs["available_total"]) * 100
                fs['ssd_percent_used'] = float(fs["used_ssd"]) / float(fs["available_ssd"]) * 100

                for fs_stat in ['available_total', 'used_total', 'available_ssd','used_ssd', 'total_percent_used', 'ssd_percent_used']:
                    metric_objs['weka_fs'].add_metric( [ str(cluster), fs["name"], fs_stat ], fs[fs_stat] )

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
        except:
            log.error("error processing alerts for cluster {}".format(str(cluster)))

        try:
            for drive in wekadata["driveList"]:
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
                        drive['disk_id'],
                        drive['vendor'],
                        drive['model'],
                        drive['serial_number'],
                        str(drive['size_bytes']),
                        drive['status'],
                        str(100 - int(drive['percentage_used']))
                    ], 1)

        except:
            log.error(f"error processing DRIVES for cluster {cluster}")

        # get all the IO stats...
        #            ['cluster','host_name','host_role','node_id','node_role','category','stat','unit']
        #
        # yes, I know it's convoluted... it was hard to write, so it *should* be hard to read. ;)
        # print(wekadata)
        log.debug(f"io stats cluster={cluster.name}")
        for category, stat_dict in self.get_weka_stat_list().items():
            log.debug(f"category={category}")
            if category in wekadata:
                log.debug(f"category {category} is in wekadata") 
                for stat, nodelist in wekadata[category].items():
                    unit = stat_dict[stat]
                    for node in nodelist:
                        try:
                            hostname = weka_maps["node-host"][node["node"]]  # save this because the syntax is gnarly
                            role_list = weka_maps["node-role"][node["node"]]
                        except Exception as exc:
                            # track = traceback.format_exc()
                            # print(track)
                            log.error(f"{exc} error in maps for cluster {str(cluster)}")
                            return  # or return? was continue

                        for role in role_list:

                            labelvalues = [
                                str(cluster),
                                hostname,
                                weka_maps["host-role"][hostname],
                                node["node"],
                                role,
                                category,
                                stat,
                                unit]

                            if unit != "sizes":
                                try:
                                    if category == 'ops_nfs':
                                        log.debug("ops_nfs is: {} {}".format(stat, node["stat_value"]))
                                    metric_objs['weka_stats_gauge'].add_metric(labelvalues, node["stat_value"],
                                                                               timestamp=wekatime_to_datetime(
                                                                                   node['timestamp']).timestamp())
                                except:
                                    print(f"{traceback.format_exc()}")
                                    log.error("error processing io stats for cluster {}".format(str(cluster)))
                            else:

                                try:
                                    if category == 'ops_nfs':
                                        log.debug("ops_nfs is: {} {}".format(stat, node["stat_value"]))
                                    release_list = cluster.release.split('.')  # 3.8.1 becomes ['3','8','1']
                                    if int(release_list[0]) >= 3 and int(release_list[1]) >= 8:
                                        value_dict, gsum = parse_sizes_values_post38(
                                            node["stat_value"])  # Turn the stat_value into a dict
                                    else:
                                        value_dict, gsum = parse_sizes_values_pre38(
                                            node["stat_value"])  # Turn the stat_value into a dict
                                    metric_objs['weka_io_histogram'].add_metric(labels=labelvalues, buckets=value_dict,
                                                                                gsum_value=gsum)
                                except:
                                    track = traceback.format_exc()
                                    print(track)
                                    log.error("error processing io sizes for cluster {}".format(str(cluster)))
            else:
                log.debug(f"category {category} is NOT in wekadata") 

        # shut down the child processes
        log.debug(f"shutting down children")
        del self.async


        log.info(f"Complete cluster={cluster}")

    # ------------- end of gather() -------------

    def collect_logs(self, lokiserver):
        with self._access_lock:  # make sure we don't conflict with a metrics collection
            log.info(f"getting events for cluster {self.cluster}")
            try:
                events = self.cluster.setup_events()    # this uses cluster api, so needs a lock
            except Exception as exc:
                log.critical(f"Error setting up for events: {exc} for cluster {self.cluster}")
                #log.critical(f"{traceback.format_exc()}")
                return

        try:
            events = self.cluster.get_events()  # this goes to weka home, so no lock needed
        except Exception as exc:
            log.critical(f"Error getting events: {exc} for cluster {self.cluster}")
            #log.critical(f"{traceback.format_exc()}")
            return

        try:
            lokiserver.send_events(events, self.cluster)
        except Exception as exc:
            log.critical(f"Error sending events: {exc} for cluster {self.cluster}")
            #log.critical(f"{traceback.format_exc()}")

    @staticmethod
    def _trim_time(time_string):
        tmp = time_string.split('.')
        return tmp[0]
