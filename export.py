# Weka Prometheus client
# Vince Fleming
# vince@weka.io
#

# It's best to run this either as a binary (use the build.sh to build it) or in the docker container.
# You can download a copy of the pre-built binary from github.com (look under Releases)
# You can download pre-built docker container from docker hub - "wekasolutions/export"

import argparse
# system imports
import logging.handlers
import os
import sys
import time
import platform
import traceback
import yaml
import socket

import prometheus_client

from events import WekaEventProcessor
# local imports
#from maps import Map, MapRegistry
from register import Registry
import wekalib.signals as signals
from collector import WekaCollector
from wekalib.wekacluster import WekaCluster
import wekalib.exceptions

VERSION = "20260109"

#VERSION = "experimental"

# set the root log
log = logging.getLogger()


# load the config file
#@staticmethod
def _load_config(inputfile):
    try:
        f = open(inputfile)
    except Exception as exc:
        raise
    with f:
        try:
            return yaml.load(f, Loader=yaml.FullLoader)
        except AttributeError:
            return yaml.load(f)
        except Exception as exc:
            log.error(f"Error reading config file: {exc}")
            raise

def prom_client(config):
    # Parse config data, set defaults for missing values
    exporter = config.get('exporter', None)
    loki = config.get('loki', None)
    cluster = config.get('cluster', None)

    if exporter is None:
        log.error('Config file is missing "exporter" configuration.  Terminating.')
        sys.exit(1)
    elif cluster is None:
        log.error('Config file is missing "cluster" configuration.  Terminating.')
        sys.exit(1)
    elif loki is None and ("loki_host" not in exporter or
                           exporter['loki_host'] is None or
                           len(exporter['loki_host']) == 0):
        log.info('Config file is missing "loki" configuration. Not sending Events to Loki')
        if 'events_to_loki' in exporter and exporter['events_to_loki']:
            log.error("events_to_loki set to True, but no Loki server defined; disabling events_to_loki")
        else:
            log.info("No Loki server defined; events_to_loki disabled")
        events_to_loki = False
    else:
        events_to_loki = exporter.get('events_to_loki', True)

    # cluster options
    cluster['force_https'] = cluster.get('force_https', False)
    cluster['verify_cert'] = cluster.get('verify_cert', True)
    cluster['mgmt_port'] = cluster.get('mgmt_port', 14000)

    # exporter options
    exporter['timeout'] = exporter.get('timeout', 10)
    exporter['backends_only'] = exporter.get('backends_only', False)
    exporter['datapoints_per_collect'] = exporter.get('datapoints_per_collect', 1)
    exporter['certfile'] = exporter.get('certfile', None)
    exporter['keyfile'] = exporter.get('keyfile', None)

    # is there a loki server set?
    if events_to_loki:
        if loki is None:
            loki = dict()
            loki['host'] = exporter.get('loki_host', None)
            loki['port'] = exporter.get('loki_port', 3100)
            loki['protocol'] = 'http'
            loki['path'] = '/loki/api/v1/push'
            loki['user'] = None
            loki['password'] = None
            loki['org_id'] = None
            loki['client_cert'] = None
            loki['verify_cert'] = False
        else:
            # set defaults, if needed
            loki['host'] = loki.get('host', None)
            loki['port'] = loki.get('port', 3100)
            loki['protocol'] = loki.get('protocol', 'http')
            loki['path'] = loki.get('path', '/loki/api/v1/push')
            loki['user'] = loki.get('user', None)
            loki['password'] = loki.get('password', None)
            loki['org_id'] = loki.get('org_id', None)
            loki['client_cert'] = loki.get('client_cert', None)
            loki['verify_cert'] = loki.get('verify_cert', False)

    events_only = exporter.get('events_only', False)
    events_to_syslog = exporter.get('events_to_syslog', False)

    # log the timeout
    log.info(f"Prometheus Exporter for Weka version {VERSION}")
    log.info("Configuration:")
    #log.info(f"config file: {config['configfile']}")
    log.info(f"cluster hosts: {cluster['hosts']}")
    if len(cluster['hosts']) <= 1:
        log.warning(f"Only one host defined - consider adding more for HA")
    log.info(f"force_https: {cluster['force_https']}")
    log.info(f"verify_cert: {cluster['verify_cert']}")
    log.info(f"mgmt_port: {cluster['mgmt_port']}")
    log.info(f"timeout: {exporter['timeout']} secs")
    log.info(f"backends_only: {exporter['backends_only']}")
    log.info(f"datapoints_per_collect: {exporter['datapoints_per_collect']}")
    log.info(f"certfile: {exporter['certfile']}")
    log.info(f"keyfile: {exporter['keyfile']}")
    #log.info(f"loki_host: {loki_host}")
    #log.info(f"loki_port: {loki_port}")
    log.info(f"events_only: {events_only}")
    log.info(f"events_to_loki: {events_to_loki}")
    log.info(f"events_to_syslog: {events_to_syslog}")
    log.info(f'loki config: {loki}')

    # check that we have name resolution
    error = False
    for host in config['cluster']['hosts']:
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            log.critical(f"Hostname {host} not resolvable - is it in /etc/hosts or DNS?")
            error = True
        except Exception as exc:
            log.critical(exc)
            error = True

    if error:
        log.critical("Errors resolving hostnames given.  Please ensure they are in /etc/hosts or DNS and are resolvable")
        sys.exit(1)
    elif 'cluster' not in config:
        log.error(f"'cluster:' stanza missing from .yml file - version mismatch between .yml and exporter version?")
        sys.exit(1)
    elif 'exporter' not in config:
        log.error(f"'exporter:' stanza missing from .yml file - version mismatch between .yml and exporter version?")
        sys.exit(1)


    try:
        cluster_obj = WekaCluster(cluster['hosts'], cluster['auth_token_file'],
                                  force_https=cluster['force_https'],
                                  verify_cert=cluster['verify_cert'],
                                  backends_only=exporter['backends_only'],
                                  timeout=exporter['timeout'],
                                  mgmt_port=cluster['mgmt_port'])
    except wekalib.exceptions.HTTPError as exc:
        if exc.code == 403:
            log.critical(f"Cluster returned permission error - is the userid level ReadOnly or above?")
            return
        log.critical(f"Cluster returned HTTP error {exc}; aborting")
        return
    except wekalib.exceptions.SSLError as exc:
        log.critical(f"SSL Error: Only weka v3.10 and above support https, and force_https is set in config file.")
        log.critical(f"SSL Error: Is this cluster < v3.10? Please verify configuration")
        log.critical(f"Error is {exc}")
        return
    except Exception as exc:
        log.critical(f"Unable to create Weka Cluster: {exc}")
        #log.critical(traceback.format_exc())
        return


    config["registry"] = Registry()

    # create the WekaCollector object
    collector = WekaCollector(config, cluster_obj)

    # create the event processor
    event_processor = WekaEventProcessor(config["registry"]) if events_to_loki or events_to_syslog else None

    if event_processor is not None:
        if events_to_loki:
            log.info("Events to Loki enabled")
            event_processor.configure_loki(loki)

        configure_event_syslog(events_to_syslog)
    else:
        if events_only:
            log.critical("events_only set, but not configured to send them anywhere")
            sys.exit(1)

    #
    # Start up the server to expose the metrics.
    #
    if not events_only:
        log.info(f"starting http server on port {config['exporter']['listen_port']}")
        try:
            if config['exporter']['certfile'] is not None and config['exporter']['keyfile'] is not None:
                prometheus_client.start_http_server(int(config['exporter']['listen_port']),
                                                    certfile=config['exporter']['certfile'],
                                                    keyfile=config['exporter']['keyfile'])
            else:
                prometheus_client.start_http_server(int(config['exporter']['listen_port']))
        except Exception as exc:
            log.critical(f"Unable to start http server on port {config['exporter']['listen_port']}: {exc}")
            return 1

        # register our custom collector
        prometheus_client.REGISTRY.register(collector)

    time.sleep(30)  # sleep first, just in case we're started at the same time as Loki; give it time
    log.info("Starting Event gathering loop")
    while True:
        time.sleep(30)  # sleep first, just in case we're started at the same time as Loki; give it time
        if event_processor is not None:
            collector.collect_logs(event_processor)

def configure_logging(logger, verbosity):
    loglevel = logging.INFO     # default logging level
    libloglevel = logging.ERROR

    # default message formats
    console_format = "%(message)s"
    #syslog_format =  "%(levelname)s:%(message)s"

    if verbosity == 1:
        loglevel = logging.INFO
        console_format = "%(levelname)s:%(message)s"
        libloglevel = logging.INFO
    elif verbosity == 2:
        loglevel = logging.DEBUG
        console_format = "%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
    elif verbosity > 2:
        loglevel = logging.DEBUG
        console_format = "%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        libloglevel = logging.DEBUG


    # create handler to log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(console_format))
    logger.addHandler(console_handler)

    # set default loglevel
    logger.setLevel(loglevel)

    logging.getLogger("wekalib").setLevel(logging.ERROR)
    logging.getLogger("wekalib.wekaapi").setLevel(libloglevel) # should leave at INFO as default
    logging.getLogger("wekalib.wekacluster").setLevel(libloglevel)
    logging.getLogger("wekalib.sthreads").setLevel(logging.ERROR) # should leave at ERROR as default
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    # local modules
    logging.getLogger("collector").setLevel(loglevel)
    logging.getLogger("lokilogs").setLevel(loglevel)
    logging.getLogger("async_api").setLevel(loglevel)


def syslog_good(syslogaddr):
    import os
    import stat
    try:
        # Get file status, does not follow symbolic links
        mode = os.lstat(syslogaddr).st_mode
    except OSError as e:
        log.info(f"Error accessing path: {e} - disabling syslog logging")
        return False
    if stat.S_ISDIR(mode):
        log.info(f"Error: {syslogaddr} is a directory - disabling syslog logging")
        return False
    return True


def configure_syslog(logger, enabled=False):
    # see if there is a handler already
    syslog_handler = get_handler(logger, logging.handlers.SysLogHandler)

    syslogaddr = "/var/run/syslog" if platform.platform()[:5] == "macOS" else "/dev/log"

    log.info(f"configuring syslog.  enabled={enabled}")

    if enabled and syslog_good(syslogaddr):
        if syslog_handler is None:
            log.info(f"enabling syslog program logging to {syslogaddr}")
            syslog_handler = logging.handlers.SysLogHandler(syslogaddr)
            logger.addHandler(syslog_handler)

        syslog_handler.setFormatter(
                logging.Formatter("%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"))
        log.info("syslog enabled")
    else:
        if syslog_handler is not None:
            logger.removeHandler(syslog_handler)
        log.info("syslog program logging disabled")


def configure_event_syslog(enabled=False):
    syslog = logging.getLogger("event_syslog")
    syslogaddr = "/var/run/syslog" if platform.platform()[:5] == "macOS" else "/dev/log"
    if enabled and syslog_good(syslogaddr):
        syslog.setLevel(logging.INFO)
        # see if there is a handler already
        syslog_handler = get_handler(syslog, logging.handlers.SysLogHandler)
        if syslog_handler is None:
            syslog_handler = logging.handlers.SysLogHandler(syslogaddr)
            syslog.addHandler(syslog_handler)
        syslog_handler.setFormatter(logging.Formatter("%(message)s"))
        log.info("event syslog enabled")
    else:
        handler = get_handler(syslog, logging.handlers.SysLogHandler)
        if handler is not None:
            syslog.removeHandler(handler)
        log.info("event syslog disabled")


def get_handler(logger, handler_type):
    for handler in logger.handlers:
        if isinstance(handler, handler_type):
            return handler
    return None


def main():
    # handle signals (ie: ^C and such)
    signals.signal_handling()

    parser = argparse.ArgumentParser(description="Prometheus Client for Weka clusters")
    parser.add_argument("-c", "--configfile", dest='configfile', default="./export.yml",
                        help="override ./export.yml as config file")
    parser.add_argument("--no_syslog", action="store_true", default=False, help="Disable syslog logging")
    parser.add_argument("-v", "--verbosity", action="count", default=0, help="increase output verbosity")
    parser.add_argument("--version", dest="version", default=False, action="store_true", help="Display version number")
    args = parser.parse_args()

    if args.version:
        print(f"{sys.argv[0]} version {VERSION}")
        sys.exit(0)

    configure_logging(log, args.verbosity)
    configure_syslog(log, not args.no_syslog)  # program syslog logging

    if not os.path.exists(args.configfile):
        log.critical(f"Required configfile '{args.configfile}' does not exist")
        sys.exit(1)

    log.debug("loading config file")
    try:
        config = _load_config(args.configfile)
    except Exception as exc:
        log.critical(f"Error loading config file '{args.configfile}': {exc}")
        return
    log.debug("config file loaded")

    # main loop
    prom_client(config)



if __name__ == '__main__':
    main()
