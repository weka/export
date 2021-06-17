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
from multiprocessing import Process
import yaml

import prometheus_client

import wekalib.signals as signals
from collector import WekaCollector
from lokilogs import LokiServer
# local imports
from wekalib.wekacluster import WekaCluster, APIException

VERSION = "1.1.0"

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

    try:
        cluster_obj = WekaCluster(config['cluster']['hosts'], config['cluster']['auth_token_file'])
    except APIException as exc:
        #track = traceback.format_exc()
        #print(track)
        if exc.message == "host_unreachable":
            log.critical(f"Unable to communicate with cluster '{config['cluster']['hosts']}': {exc.message}.  Are the cluster's hostnames in /etc/hosts and/or DNS?")
        else:
            log.critical(f"Unable to communicate with cluster '{config['cluster']['hosts']}': {exc.message}.  Is the auth file is up-to-date?")
        return
    except Exception as exc:
        # misc errors
        log.critical(f"Misc error creating cluster object with cluster '{config['cluster']['hosts']}': {exc}.  Is the cluster down?")
        log.debug(traceback.format_exc())
        return

    # create the WekaCollector object
    collector = WekaCollector(config, cluster_obj)

    if config['exporter']['loki_host'] is not None:
        lokiserver = LokiServer(config['exporter']['loki_host'], config['exporter']['loki_port'])
    else:
        lokiserver = None

    #
    # Start up the server to expose the metrics.
    #
    log.info(f"starting http server on port {config['exporter']['listen_port']}")
    try:
        prometheus_client.start_http_server(int(config['exporter']['listen_port']))
    except Exception as exc:
        log.critical(f"Unable to start http server on port {config['exporter']['listen_port']}: {exc}")
        return 1

    # register our custom collector
    prometheus_client.REGISTRY.register(collector)

    while True:
        time.sleep(30)  # sleep first, just in case we're started at the same time as Loki; give it time
        if lokiserver is not None:
            log.info(f"getting events for cluster {cluster_obj.name}")
            try:
                events = cluster_obj.get_events()
            except Exception as exc:
                log.critical(f"Error getting events: {exc} for cluster {cluster_obj.name}")
                #log.critical(f"{traceback.format_exc()}")
            else:
                try:
                    lokiserver.send_events(events, cluster_obj)
                except Exception as exc:
                    log.critical(f"Error sending events: {exc} for cluster {cluster_obj.name}")
                    #log.critical(f"{traceback.format_exc()}")


def configure_logging(logger, verbosity):
    loglevel = logging.INFO     # default logging level

    # default message formats
    console_format = "%(message)s"
    syslog_format =  "%(levelname)s:%(message)s"

    syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"

    if verbosity == 1:
        loglevel = logging.DEBUG
        console_format = "%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
    elif verbosity > 1:
        loglevel = logging.DEBUG
        console_format = "%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"

    # create handler to log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(console_format))
    logger.addHandler(console_handler)

    # create handler to log to syslog
    logger.info(f"setting syslog on {platform.platform()}")
    if platform.platform()[:5] == "macOS":
        syslogaddr = "/var/run/syslog"
    else:
        syslogaddr = "/dev/log"
    syslog_handler = logging.handlers.SysLogHandler(syslogaddr)
    syslog_handler.setFormatter(logging.Formatter(syslog_format))

    # add syslog handler to root logger
    if syslog_handler is not None:
        logger.addHandler(syslog_handler)

    # set default loglevel
    logger.setLevel(loglevel)

    logging.getLogger("wekalib").setLevel(logging.ERROR)
    logging.getLogger("wekalib.wekaapi").setLevel(logging.INFO) # should leave at INFO as default
    logging.getLogger("wekalib.wekacluster").setLevel(logging.INFO)
    logging.getLogger("wekalib.sthreads").setLevel(logging.ERROR) # should leave at ERROR as default
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    # local modules
    logging.getLogger("collector").setLevel(logging.INFO)
    logging.getLogger("lokilogs").setLevel(logging.INFO)


def main():
    # handle signals (ie: ^C and such)
    signals.signal_handling()

    parser = argparse.ArgumentParser(description="Prometheus Client for Weka clusters")
    parser.add_argument("-c", "--configfile", dest='configfile', default="./export.yml",
                        help="override ./export.yml as config file")
    parser.add_argument("-v", "--verbosity", action="count", default=0, help="increase output verbosity")
    parser.add_argument("--version", dest="version", default=False, action="store_true", help="Display version number")
    args = parser.parse_args()

    if args.version:
        print(f"{sys.argv[0]} version {VERSION}")
        sys.exit(0)

    configure_logging(log, args.verbosity)

    #if not os.path.exists(args.configfile):
    #    log.critical(f"Required configfile '{args.configfile}' does not exist")
    #    sys.exit(1)

    log.debug("loading config file")
    try:
        config = _load_config(args.configfile)
    except Exception as exc:
        log.critical(f"Error loading config file '{args.configfile}': {exc}")
        return
    log.debug("config file loaded")

    prom_client(config)



if __name__ == '__main__':
    main()
