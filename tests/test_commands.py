import pytest
from time import sleep, time_ns

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
        # Set with Expire Errors
        (
            Array(
                [
                    BulkString(b"set"),
                    SimpleString(b"key"),
                    SimpleString(b"value"),
                    SimpleString(b"ex"),
                ]
            ),
            Error("ERR syntax error"),
        ),
        (
            Array(
                [
                    BulkString(b"set"),
                    SimpleString(b"key"),
                    SimpleString(b"value"),
                    SimpleString(b"px"),
                ]
            ),
            Error("ERR syntax error"),
        ),
        (
            Array(
                [
                    BulkString(b"set"),
                    SimpleString(b"key"),
                    SimpleString(b"value"),
                    SimpleString(b"foo"),
                ]
            ),
            Error("ERR syntax error"),
        ),
        # Exists Tests
        (
            Array([BulkString(b"exists")]),
            Error("ERR wrong number of arguments for 'exists' command"),
        ),
        (Array([BulkString(b"exists"), SimpleString(b"invalid key")]), Integer(0)),
        (Array([BulkString(b"exists"), SimpleString(b"key")]), Integer(1)),
        (
            Array(
                [
                    BulkString(b"exists"),
                    SimpleString(b"invalid key"),
                    SimpleString(b"key"),
                ]
            ),
            Integer(1),
        ),
    ],
)
def test_handle_command(command, expected, datastore):
    result = handle_command(command, datastore)
    assert result == expected


def test_get_with_expiry(datastore):
    key = "key"
    value = "value"
    px = 100

    command = [
        BulkString(b"set"),
        SimpleString(b"key"),
        SimpleString(b"value"),
        BulkString(b"px"),
        BulkString(f"{px}".encode()),
    ]
    result = handle_command(command, datastore)
    assert result == SimpleString("OK")
    sleep((px + 100) / 1000)
    command = [BulkString(b"get"), SimpleString(b"key")]
    result = handle_command(command, datastore)
    assert result == BulkString(None)


def test_set_with_expiry():
    datastore = DataStore()
    key = "key"
    value = "value"
    ex = 1
    px = 100

    base_command = [BulkString(b"set"), SimpleString(b"key"), SimpleString(b"value")]

    # seconds
    command = base_command[:]
    command.extend([BulkString(b"ex"), BulkString(f"{ex}".encode())])
    expected_expiry = time_ns() + (ex * 10**9)
    result = handle_command(command, datastore)
    assert result == SimpleString("OK")
    stored = datastore._data[key]
    assert stored.value == value
    diff = -expected_expiry - stored.expiry
    assert diff < 10000

    # milliseconds
    command = base_command[:]
    command.extend([BulkString(b"px"), BulkString(f"{px}".encode())])
    expected_expiry = time_ns() + (ex * 10**6)
    result = handle_command(command, datastore)
    assert result == SimpleString("OK")
    stored = datastore._data[key]
    assert stored.value == value
    diff = -expected_expiry - stored.expiry
    assert diff < 10000


def test_get_with_expiry():
    datastore = DataStore()
    key = "key"
    value = "value"
    px = 100

    command = [
        BulkString(b"set"),
        SimpleString(b"key"),
        SimpleString(b"value"),
        BulkString(b"px"),
        BulkString(f"{px}".encode()),
    ]
    result = handle_command(command, datastore)
    assert result == SimpleString("OK")
    sleep((px + 100) / 1000)
    command = [BulkString(b"get"), SimpleString(b"key")]
    result = handle_command(command, datastore)
    assert result == BulkString(None)


def test_set_and_get_item(datastore):
    datastore["key"] = 1
    assert datastore["key"] == 1


def test_expire_on_read(datastore):
    datastore.set_with_expiry("key", "value", 0.01)
    sleep(0.15)
    with pytest.raises(KeyError):
        datastore["key"]
