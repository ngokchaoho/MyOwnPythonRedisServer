import pytest
from time import sleep, time_ns

from pyredis.commands import handle_command
from pyredis.persistence import AppendOnlyPersister
from pyredis.datastore import DataStore
from pyredis.types import Array, BulkString, Error, Integer, SimpleString

from collections import deque


@pytest.fixture(scope="module")
def datastore():
    datastore = DataStore()
    datastore["always_exist_key"] = "default"
    return datastore


@pytest.fixture(scope="module")
def persister():
    persister = AppendOnlyPersister("test.aof")
    return persister


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
        # DEL tests
        (
            Array([BulkString(b"SET"), SimpleString(b"key1"), SimpleString(b"Hello")]),
            SimpleString("OK"),
        ),
        (
            Array([BulkString(b"SET"), SimpleString(b"key2"), SimpleString(b"World")]),
            SimpleString("OK"),
        ),
        (
            Array(
                [
                    BulkString(b"DEL"),
                    SimpleString(b"key1"),
                    SimpleString(b"key2"),
                    SimpleString(b"key3"),
                ]
            ),
            Integer(2),
        ),
        # Incr Tests
        (
            Array([BulkString(b"incr")]),
            Error("ERR wrong number of arguments for 'incr' command"),
        ),
        (
            Array([BulkString(b"incr"), SimpleString(b"key")]),
            Error("ERR value is not an integer or out of range"),
        ),
        # Decr Tests
        (
            Array([BulkString(b"decr")]),
            Error("ERR wrong number of arguments for 'decr' command"),
        ),
        # Lpush Tests
        (
            Array([BulkString(b"lpush")]),
            Error("ERR wrong number of arguments for 'lpush' command"),
        ),
        # Rpush Tests
        (
            Array([BulkString(b"rpush")]),
            Error("ERR wrong number of arguments for 'rpush' command"),
        ),
    ],
)
def test_handle_command(command, expected, datastore, persister):
    result = handle_command(command, datastore, persister)
    assert result == expected


def test_get_with_expiry(datastore, persister):
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
    result = handle_command(command, datastore, persister)
    assert result == SimpleString("OK")
    sleep((px + 100) / 1000)
    command = [BulkString(b"get"), SimpleString(b"key")]
    result = handle_command(command, datastore, persister)
    assert result == BulkString(None)


def test_set_with_expiry(persister):
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
    result = handle_command(command, datastore, persister)
    assert result == SimpleString("OK")
    stored = datastore._data[key]
    assert stored.value == value
    diff = -expected_expiry - stored.expiry
    assert diff < 10000

    # milliseconds
    command = base_command[:]
    command.extend([BulkString(b"px"), BulkString(f"{px}".encode())])
    expected_expiry = time_ns() + (ex * 10**6)
    result = handle_command(command, datastore, persister)
    assert result == SimpleString("OK")
    stored = datastore._data[key]
    assert stored.value == value
    diff = -expected_expiry - stored.expiry
    assert diff < 10000


def test_get_with_expiry(persister):
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
    result = handle_command(command, datastore, persister)
    assert result == SimpleString("OK")
    sleep((px + 100) / 1000)
    command = [BulkString(b"get"), SimpleString(b"key")]
    result = handle_command(command, datastore, persister)
    assert result == BulkString(None)


def test_set_and_get_item(datastore):
    datastore["key"] = 1
    assert datastore["key"] == 1


def test_expire_on_read(datastore):
    datastore.set_with_expiry("key", "value", 0.01)
    sleep(0.15)
    with pytest.raises(KeyError):
        datastore["key"]


# Incr Tests
def test_handle_incr_command_valid_key(persister):
    datastore = DataStore()
    result = handle_command(
        Array([BulkString(b"incr"), SimpleString(b"ki")]), datastore, persister
    )
    assert result == Integer(1)
    result = handle_command(
        Array([BulkString(b"incr"), SimpleString(b"ki")]), datastore, persister
    )
    assert result == Integer(2)


# Decr Tests
def test_handle_decr(persister):
    datastore = DataStore()
    result = handle_command(
        Array([BulkString(b"incr"), SimpleString(b"kd")]), datastore, persister
    )
    assert result == Integer(1)
    result = handle_command(
        Array([BulkString(b"incr"), SimpleString(b"kd")]), datastore, persister
    )
    assert result == Integer(2)
    result = handle_command(
        Array([BulkString(b"decr"), SimpleString(b"kd")]), datastore, persister
    )
    assert result == Integer(1)
    result = handle_command(
        Array([BulkString(b"decr"), SimpleString(b"kd")]), datastore, persister
    )
    assert result == Integer(0)


# Lpush Tests
def test_handle_lpush_lrange(persister):
    datastore = DataStore()
    result = handle_command(
        Array([BulkString(b"lpush"), SimpleString(b"klp"), SimpleString(b"second")]),
        datastore,
        persister,
    )
    assert result == Integer(1)
    result = handle_command(
        Array([BulkString(b"lpush"), SimpleString(b"klp"), SimpleString(b"first")]),
        datastore,
        persister,
    )
    assert result == Integer(2)
    result = handle_command(
        Array(
            [
                BulkString(b"lrange"),
                SimpleString(b"klp"),
                BulkString(b"0"),
                BulkString(b"2"),
            ]
        ),
        datastore,
        persister,
    )
    assert result == Array(data=[BulkString("first"), BulkString("second")])


# Rpush Tests
def test_handle_rpush_lrange(persister):
    datastore = DataStore()
    result = handle_command(
        Array([BulkString(b"rpush"), SimpleString(b"krp"), SimpleString(b"first")]),
        datastore,
        persister,
    )
    assert result == Integer(1)
    result = handle_command(
        Array([BulkString(b"rpush"), SimpleString(b"krp"), SimpleString(b"second")]),
        datastore,
        persister,
    )
    assert result == Integer(2)
    result = handle_command(
        Array(
            [
                BulkString(b"lrange"),
                SimpleString(b"krp"),
                BulkString(b"0"),
                BulkString(b"2"),
            ]
        ),
        datastore,
        persister,
    )
    assert result == Array(data=[BulkString("first"), BulkString("second")])


@pytest.fixture
def ds():
    return DataStore()


def test_initial_data_invalid_type():
    with pytest.raises(TypeError):
        ds = DataStore("string")


def test_initial_data():
    ds = DataStore({"k1": 1, "k2": "v2"})
    assert ds["k1"] == 1
    assert ds["k2"] == "v2"


def test_in(ds):
    ds["key"] = 1

    assert "key" in ds
    assert "key2" not in ds


def test_get_item(ds):
    ds["key"] = 1
    assert ds["key"] == 1


def test_set_item(ds):
    l = ds.append("key", 1)
    assert l == 1
    assert ds["key"] == deque([1])


def test_incr(ds):
    ds["k"] = "1"
    res = ds.incr("k")
    assert res == 2
    res = ds.incr("k")
    assert res == 3


def test_decr(ds):
    ds["k"] = "1"
    ds.incr("k")
    ds.incr("k")
    res = ds.incr("k")
    assert res == 4
    res = ds.decr("k")
    assert res == 3
    res = ds.decr("k")
    assert res == 2


def test_append(ds):
    num_entries = ds.append("key", 1)
    assert num_entries == 1
    assert ds["key"] == deque([1])


def test_preppend(ds):
    ds.append("key", 1)
    ds.prepend("key", 2)
    assert ds["key"] == deque([2, 1])


def test_expire_on_read(ds):
    ds.set_with_expiry("key", "value", 0.01)
    sleep(0.15)
    with pytest.raises(KeyError):
        ds["key"]


def test_remove_expired_keys_empty():
    ds = DataStore()
    ds.remove_expired_keys()


def _fill_ds(ds, size, percent_expired):
    num_expired = int(size * (percent_expired / 100))

    # items without expiry
    for i in range(size - num_expired):
        ds[f"{i}"] = i

    # items with expiry and that will have expired
    for i in range(num_expired):
        ds.set_with_expiry(f"e_{i}", i, -1)


@pytest.mark.parametrize("size, percent_expired", [(20, 10), (200, 100)])
def test_remove_expired_keys(size, percent_expired):
    expected_len_after_expiry = size - (size * (percent_expired / 100))

    ds = DataStore()
    _fill_ds(ds, size, percent_expired)

    ds.remove_expired_keys()
    assert len(ds._data) == expected_len_after_expiry
