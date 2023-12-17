from threading import Lock
from dataclasses import dataclass
from typing import Any
from time import time

import random
import logging


EXPIRY_TEST_SAMPLE_SIZE = 20
log = logging.getLogger("pyredis")


@dataclass
class DataEntry:
    """Class to represent a data entry. Contains the data and the expiry in milisecond."""

    value: Any
    expiry: int = 0


class DataStore:
    """
    The core data store, provides a thread safe dictionary extended with
    the interface needed to support Redis functionality.
    """

    def __init__(self, initial_data=None):
        self._data: dict[str, DataEntry] = dict()
        self._lock = Lock()
        if initial_data:
            if not isinstance(initial_data, dict):
                raise TypeError("Initial Data should be of type dict")

            for key, value in initial_data.items():
                self._data[key] = DataEntry(value)

    def __getitem__(self, key):
        with self._lock:
            log.info("Try to get key %s", key)
            item = self._data[key]
            log.info("key exist %s, checking expiry", key)
            # if key expired
            if self.check_expiry(key, item):
                raise KeyError  # catched in _handle_get

            return item.value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = DataEntry(value)

    def set_with_expiry(self, key, value, expiry: int):
        with self._lock:
            calculated_expiry = int(time() * 1000) + expiry  # in miliseconds
            self._data[key] = DataEntry(value, calculated_expiry)

    def check_expiry(self, key: str, value: DataEntry) -> bool:
        # if key expired then delete
        if value.expiry and value.expiry < int(time() * 1000):
            log.info("%s key expired", key)
            del self._data[key]
            return True
        else:
            return False

    def remove_expired_keys(self):
        expired_count = 0
        with self._lock:
            keys = random.sample(
                sorted(self._data), min(EXPIRY_TEST_SAMPLE_SIZE, len(self._data))
            )

            for key in keys:
                if self.check_expiry(key, self._data[key]):
                    expired_count += 1
        # after release lock
        # if more than
        if expired_count > EXPIRY_TEST_SAMPLE_SIZE * 0.25:
            self.remove_expired_keys()
