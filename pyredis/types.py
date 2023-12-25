from collections.abc import Sequence
from dataclasses import dataclass


@dataclass
class SimpleString:
    data: str

    def resp_encode(self):
        return f"+{self.data}\r\n".encode()


@dataclass
class Error:
    data: str

    def resp_encode(self):
        return f"-{self.data}\r\n".encode()


@dataclass
class Integer:
    data: int

    def resp_encode(self):
        return f":{self.data}\r\n".encode()


@dataclass
class BulkString:
    data: bytes

    def resp_encode(self):
        # NULL bulk String
        if self.data is None:
            return "$-1\r\n".encode()
        else:
            if isinstance(self.data, str):
                return f"${len(self.data)}\r\n{self.data}\r\n".encode()
            else:
                return (
                    f"${len(self.data)}\r\n".encode()
                    + bytes(self.data)
                    + "\r\n".encode()
                )


@dataclass
class Array(Sequence):
    data: list

    def __getitem__(self, i):
        return self.data[i]

    def __len__(self):
        return len(self.data)

    def resp_encode(self):
        # NULL array
        if self.data is None:
            return b"*-1\r\n"
        str_representation = [
            f"*{len(self.data)}\r\n".encode(),
        ]
        for frame in self.data:
            str_representation.append(frame.resp_encode())

        return b"".join(str_representation)
