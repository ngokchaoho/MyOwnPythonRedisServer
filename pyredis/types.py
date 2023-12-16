from collections.abc import Sequence
from dataclasses import dataclass


@dataclass
class SimpleString:
    data: str

@dataclass
class Error:
    data:str

@dataclass
class Integer:
    data:int

@dataclass
class BulkString:
    data:bytes

@dataclass
class Array(Sequence):
    data: list

    def __getitem__(self, i):
        return self.data[i]

    def __len__(self):
        return len(self.data)

