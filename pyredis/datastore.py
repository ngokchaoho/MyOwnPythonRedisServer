from threading import Lock
from dataclasses import dataclass
from typing import Any
from time import time


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
            item = self._data[key]

            # if key expired
            if item.expiry and item.expiry < int(time() * 1000):
                del self._data[key]
                raise KeyError

            return item.value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = DataEntry(value)

    def set_with_expiry(self, key, value, expiry: int):
        with self._lock:
            calculated_expiry = int(time() * 1000) + expiry  # in miliseconds
            self._data[key] = DataEntry(value, calculated_expiry)

    def check_expiry(datastore):
        pass
