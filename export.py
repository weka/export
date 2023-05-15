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
import socket

import prometheus_client

# local imports
#from maps import Map, MapRegistry
from maps import MapRegistry
import wekalib.signals as signals
from collector import WekaCollector
from lokilogs import LokiServer
from wekalib.wekacluster import WekaCluster
import wekalib.exceptions

VERSION = "1.6.7"

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

    if 'force_https' not in config['cluster']:  # allow defaults for these
        config['cluster']['force_https'] = False

    if 'verify_cert' not in config['cluster']:
        config['cluster']['verify_cert'] = True

    if 'timeout' not in config['exporter']:
        config['exporter']['timeout'] = 10

    if 'backends_only' not in config['exporter']:
        config['exporter']['backends_only'] = False

    if 'datapoints_per_collect' not in config['exporter']:
        config['exporter']['datapoints_per_collect'] = 1

    log.info(f"Timeout set to {config['exporter']['timeout']} secs")

    try:
        cluster_obj = WekaCluster(config['cluster']['hosts'], config['cluster']['auth_token_file'], 
                                  force_https=config['cluster']['force_https'], 
                                  verify_cert=config['cluster']['verify_cert'], 
                                  backends_only=config['exporter']['backends_only'],
                                  timeout=config['exporter']['timeout'])
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
        log.critical(traceback.format_exc())
        return

    maps = MapRegistry()
    config["map_registry"] = maps

    # create the WekaCollector object
    collector = WekaCollector(config, cluster_obj)

    # is there a loki server set?
    if config['exporter']['loki_host'] is not None and len(config['exporter']['loki_host']) != 0:
        try:
            lokiserver = LokiServer(config['exporter']['loki_host'], config['exporter']['loki_port'], maps)
        except:
            sys.exit(1)
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
            collector.collect_logs(lokiserver)


def configure_logging(logger, verbosity, disable_syslog=False):
    loglevel = logging.INFO     # default logging level
    libloglevel = logging.ERROR

    # default message formats
    console_format = "%(message)s"
    syslog_format =  "%(levelname)s:%(message)s"

    syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"

    if verbosity == 1:
        loglevel = logging.INFO
        console_format = "%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        libloglevel = logging.INFO
    elif verbosity == 2:
        loglevel = logging.DEBUG
        console_format = "%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
    elif verbosity > 2:
        loglevel = logging.DEBUG
        console_format = "%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        syslog_format =  "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
        libloglevel = logging.DEBUG


    # create handler to log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(console_format))
    logger.addHandler(console_handler)

    if not disable_syslog:
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
    logging.getLogger("wekalib.wekaapi").setLevel(libloglevel) # should leave at INFO as default
    logging.getLogger("wekalib.wekacluster").setLevel(libloglevel)
    logging.getLogger("wekalib.sthreads").setLevel(logging.ERROR) # should leave at ERROR as default
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    # local modules
    logging.getLogger("collector").setLevel(loglevel)
    logging.getLogger("lokilogs").setLevel(loglevel)
    logging.getLogger("async_api").setLevel(loglevel)


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

    configure_logging(log, args.verbosity, disable_syslog=args.no_syslog)

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

    prom_client(config)



if __name__ == '__main__':
    main()
