import logging
from collections import defaultdict
from abc import abstractmethod


class Storage:
    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def get(self, key):
        pass


class Cache(Storage):
    def __init__(self, max_entries: int, storage: Storage):
        self.storage = storage
        self.max_entries = max_entries
        self.entries = {}
        self.read_times = defaultdict(int)
        self.time = 1

    def __move_time(self):
        self.time += 1

    def __mark_entry_read(self, key):
        self.read_times[key] = self.time
        self.__move_time()

    def __add_entry(self, key, value):
        logging.info('inserting {} to cache {}'.format(key, self))
        if len(self.entries.keys()) == self.max_entries:
            logging.info('max entries reached for cache {}'.format(self))
            self.evict()
        self.entries[key] = value
        self.__mark_entry_read(key)

    def put(self, key, value):
        self.storage.put(key, value)
        self.__add_entry(key, value)

    def get(self, key):
        if key in self.entries:
            return self.entries[key]
        logging.info('miss for key {} from cache {}'.format(key, self))
        self.__add_entry(key, self.storage.get(key))
        return self.entries.get(key)

    def evict(self):
        flipped_read_times = {v: k for k, v in self.read_times.items()}
        lr_time = min(flipped_read_times.keys())
        logging.info('evicting {} from cache {}'.format(flipped_read_times[lr_time], self))
        del self.entries[flipped_read_times[lr_time]]
        del self.read_times[flipped_read_times[lr_time]]
