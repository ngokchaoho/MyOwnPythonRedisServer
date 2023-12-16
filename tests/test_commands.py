import pytest

from pyredis.commands import handle_command
from pyredis.datastore import DataStore
from pyredis.types import Array, BulkString, Error, Integer, SimpleString


@pytest.fixture(scope="module")
def datastore():
    datastore = DataStore()
    datastore["always_exist_key"] = "default"
    return datastore


@pytest.mark.parametrize(
    "command, expected",
    [
        # Echo Tests
        (
            Array([BulkString(b"ECHO")]),
            Error("ERR wrong number of arguments for 'echo' command"),
        ),
        (Array([BulkString(b"echo"), BulkString(b"Hello")]), BulkString("Hello")),
        (
            Array([BulkString(b"echo"), BulkString(b"Hello"), BulkString("World")]),
            Error("ERR wrong number of arguments for 'echo' command"),
        ),
        # Ping Tests
        (Array([BulkString(b"ping")]), SimpleString("PONG")),
        # Set Tests
        (Array([BulkString(b"ping"), BulkString(b"Hello")]), BulkString("Hello")),
        (
            Array([BulkString(b"set")]),
            Error("ERR wrong number of arguments for 'set' command"),
        ),
        (
            Array([BulkString(b"set"), SimpleString(b"key")]),
            Error("ERR wrong number of arguments for 'set' command"),
        ),
        (
            Array([BulkString(b"set"), SimpleString(b"key"), SimpleString(b"value")]),
            SimpleString("OK"),
        ),
        (
            Array([BulkString(b"get"), SimpleString(b"always_exist_key")]),
            BulkString("default"),
        ),
    ],
)
def test_handle_command(command, expected, datastore):
    result = handle_command(command, datastore)
    assert result == expected
