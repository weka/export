#
# maps - objects that map things (node ids to hostnames, etc)
#
from threading import Lock


class MapRegistry(object):
    def __init__(self):
        self._lock = Lock()
        self.map_registry = dict()

    def register(self, map_name, map_object):
        with self._lock:
            self.map_registry[map_name] = map_object

    def lookup(self, map_name):
        with self._lock:
            return self.map_registry[map_name]
