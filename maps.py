#
# maps - objects that map things (node ids to hostnames, etc)
#
from threading import Lock
import time

from logging import getLogger
log = getLogger(__name__)

class MapRegistry(object):
    def __init__(self):
        self._lock = Lock()
        self.map_registry = dict()
        self.timestamp = dict()

    def register(self, map_name, map_object):
        with self._lock:
            self.map_registry[map_name] = map_object
            self.timestamp[map_name] = time.time()

    def lookup(self, map_name):
        with self._lock:
            try:
                return self.map_registry[map_name]
            except Exception as exc:
                log.error(f"Exception looking up {map_name} map: {exc}")
                return None

    def get_timestamp(self, map_name):
        with self._lock:
            try:
                return self.timestamp[map_name]
            except Exception as exc:
                log.error(f"Exception looking up timestamp for {map_name} map: {exc}")
                return None

    def get_age(self, map_name):
        with self._lock:
            try:
                return time.time() - self.timestamp[map_name]
            except Exception as exc:
                log.error(f"Exception looking up age for {map_name} map: {exc}")
                return None