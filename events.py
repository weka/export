#! /usr/bin/env python3

#
# lokilogs - send weka cluster events to a Loki server
#
# Author: Vince Fleming, vince@weka.io
#

# example of usage grafana/loki api when you need push any log/message from your python scipt
import argparse
import json
import time
import socket
from logging import getLogger, INFO

import requests
import urllib3

from wekalib.wekatime import lokitime_to_wekatime, wekatime_to_datetime, lokitime_to_datetime, datetime_to_lokitime, datetime_to_wekatime

log = getLogger(__name__)
syslog = getLogger("event_syslog")


class WekaEventProcessor(object):
    def __init__(self, map_registry):
        self.loki = False
        self.host = ""
        self.port = 0
        self.registry = map_registry


    def configure_loki(self, lokihost, lokiport):
        self.loki = True
        self.host = lokihost
        self.port = lokiport
        # save some trouble, and make sure names are resolvable
        try:
            socket.gethostbyname(lokihost)
        except socket.gaierror as exc:
            log.critical(f"Loki Server name '{lokihost}' is not resolvable - is it in /etc/hosts or DNS?")
            raise
        except Exception as exc:
            log.critical(exc)
            raise

    # push msg log into grafana-loki
    def loki_logevent(self, timestamp, event, **labels):
        url = 'http://' + self.host + ':' + str(self.port) + '/loki/api/v1/push'  # set the URL

        # set the headers
        headers = {
            'Content-type': 'application/json'
        }

        log.debug(f"{labels}")
        # set the payload
        payload = {
            'streams': [
                {
                    'stream': labels["labels"],
                    'values': [
                        [timestamp, event]
                    ]
                }
            ]
        }

        # encode payload to a string
        payload_str = json.dumps(payload)
        # log.debug( json.dumps(payload, indent=4, sort_keys=True) )

        # this is where we actually send it
        try:
            answer = requests.post(url, data=payload_str, headers=headers)
        except requests.exceptions.ConnectionError as exc:
            log.critical(f"Unable to send Events to Loki: unable to establish connection: FATAL")
            return False
        except Exception as exc:
            log.critical(f"Unable to send Events to Loki: {exc}")
            return False

        log.debug(f"status code: {answer.status_code}")
        # check the return code
        if answer.status_code == 400:
            # I've only seen code 400 for duplicate entries; but I could be wrong. ;)
            log.error(f"Error posting event; possible duplicate entry: {answer.text}")
            return False
        elif answer.status_code != 204:  # 204 is ok
            log.error("loki_logevent(): bad http status code: " + str(answer.status_code) + " " + answer.text)
            return False

        return True

        # end loki_logevent

    # format the events and send them up to Loki
    def send_events(self, event_dict, cluster, collector):
        MINS = 60
        if self.registry.lookup('node-host') is None or self.registry.get_age('node-host') > 5 * MINS:
            log.info(f"node-host map not populated... populating")
            collector.create_maps()
            log.info(f"node-host map populated.")

        node_host_map = self.registry.lookup('node-host')
        if node_host_map is None:
            log.error(f"Unable to populate node-host map: {exc}")

        log.debug(f"node-host map age: {round(self.registry.get_age('node-host'),1)} seconds")

        if len(event_dict) == 0:
            log.debug("No events to send")
            return

        # must be sorted by timestamp or Loki will reject them
        num_successful = 0
        for timestamp, event in sorted(event_dict.items()):  # oldest first
            labels = {
                "source": "weka",
                "cluster": cluster.name,
                "category": event["category"],
                "event_type": event["type"],
                "severity": event["severity"]
            }
            # "node_id": event["nid"],
            if 'params' in event:
                params = event['params']
                log.debug(f'{event["description"]}:::::{params}')
                if 'hostname' in params:
                    labels['hostname'] = params['hostname']
                if 'nodeId' in params:
                    labels['nodeid'] = str(params['nodeId'])
                    if 'hostname' not in labels:
                        if type(params['nodeId']) is not str:
                            formatted_nodeid = 'NodeId<' + f'{params["nodeId"]}>'
                        else:
                            formatted_nodeid = params['nodeId']
                        try:
                            hostname = node_host_map[formatted_nodeid]
                        except Exception as exc:
                            log.error(f"NodeId {formatted_nodeid} not in node-host map!")
                            hostname = 'error'
                            log.debug("setting hostname*************************************************************")
                        labels['hostname'] = hostname

            # map weka event severities to Loki event severities
            orig_sev = event['severity']
            if event['severity'] == 'MAJOR' or event['severity'] == 'MINOR':
                event['severity'] = 'ERROR'
            elif event['severity'] == 'CRITICAL':
                event['severity'] = 'FATAL'

            description = f"cluster:{cluster.name} :{orig_sev}: {event['type']}: {event['description']}"
            log.debug(f"sending event: timestamp={timestamp}, labels={labels}, desc={description}")

            syslog.info(f"WekaEvent: {description}") # send to syslog, if configured

            success = False
            if self.loki:
                success = self.loki_logevent(timestamp, description, labels=labels)

            if success:
                num_successful += 1

            cluster.last_event_timestamp = event['timestamp']

        log.info(f"Total events={len(event_dict)}; successfully sent {num_successful} events to Loki")
        #if self.loki and num_successful != 0:
        #    cluster.last_event_timestamp = cluster.last_get_events_time


if __name__ == '__main__':

    # Globals
    target_host = ""
    target_port = 0
    loki_host = ""
    verbose = 0

    parser = argparse.ArgumentParser(description="Loki Log Exporter for Weka clusters")
    parser.add_argument("-c", "--configfile", dest='configfile', default="./weka-metrics-exporter.yml",
                        help="override ./weka-metrics-exporter.yml as config file")
    parser.add_argument("-p", "--port", dest='port', default="3100", help="TCP port number to listen on")
    parser.add_argument("-H", "--HOST", dest='wekahost', default="localhost",
                        help="Specify the Weka host(s) (hostname/ip) to collect stats from. May be a comma-separated list")
    parser.add_argument("-L", "--LOKIHOST", dest='lokihost', default="localhost",
                        help="Specify the hostname of the Loki server")
    parser.add_argument("-v", "--verbose", dest='verbose', action="count", help="Enable verbose output")
    args = parser.parse_args()

    target_host = args.wekahost  # make sure we can give a list in case one or more are not reachable
    target_port = args.port
    loki_host = args.lokihost
    verbose = args.verbose

    # initially, make sure we seed the list with all past events
    all_events = gather_weka_events(target_host)
    sortable_events = reformat_events(all_events)
    send_events(loki_host, sortable_events)

    while True:
        if verbose > 0:
            print("sleeping")
        time.sleep(60)  # check for events once per minute
        if verbose > 0:
            print("gathering events")
        all_events = gather_weka_events(target_host, "10m")  # maybe make this less?  1m?
        sortable_events = reformat_events(all_events)
        if verbose > 0:
            print("sending events")
        send_events(loki_host, sortable_events)
