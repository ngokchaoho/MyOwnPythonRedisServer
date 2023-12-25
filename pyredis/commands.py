from pyredis.types import Array, BulkString, Error, Integer, SimpleString
import logging

log = logging.getLogger("pyredis")


def _handle_echo(command):
    if len(command) == 2:
        message = command[1].data.decode()
        return BulkString(f"{message}")
    return Error("ERR wrong number of arguments for 'echo' command")


def _handle_ping(command):
    if len(command) == 2:
        message = command[1].data.decode()
        return BulkString(f"{message}")
    elif len(command) == 1:
        return SimpleString("PONG")
    else:
        return Error(data="ERR wrong number of arguments for 'ping' command")


def _handle_set(command, datastore, persister):
    length = len(command)
    if length >= 3:
        key = command[1].data.decode()
        value = command[2].data.decode()

        if length == 3:
            datastore[key] = value
            if persister:
                persister.log_command(command)
            return SimpleString("OK")
        elif length == 5:
            expiry_mode = command[3].data.decode()
            try:
                expiry = int(command[4].data.decode())
            except ValueError:
                return Error("ERR value is not an integer or out of range")

            if expiry_mode == "ex":
                datastore.set_with_expiry(key, value, expiry * 1000)
                if persister:
                    persister.log_command(command)
                return SimpleString("OK")
            elif expiry_mode == "px":
                datastore.set_with_expiry(key, value, expiry)
                if persister:
                    persister.log_command(command)
                return SimpleString("OK")
        return Error("ERR syntax error")

    return Error("ERR wrong number of arguments for 'set' command")


def _handle_get(command, datastore):
    if len(command) == 2:
        key = command[1].data.decode()
        try:
            value = datastore[key]
        except KeyError:
            return BulkString(None)
        return BulkString(value)
    return Error("ERR wrong numer of arguments for 'get' command")


def _handle_exists(command, datastore):
    if len(command) >= 2:
        found = 0
        for key in command[1:]:
            if key.data.decode() in datastore:
                found += 1
        return Integer(found)
    else:
        return Error("ERR wrong number of arguments for 'exists' command")


def _handle_del(command, datastore, persister):
    if len(command) >= 2:
        found = 0
        for key in command[1:]:
            if key.data.decode() in datastore:
                del datastore._data[key.data.decode()]
                found += 1
        if persister:
            persister.log_command(command)
        return Integer(found)
    else:
        return Error("ERR wrong number of arguments for 'del' command")


def _handle_incr(command, datastore, persister):
    if len(command) == 2:
        key = command[1].data.decode()
        try:
            value = datastore.incr(key)
            if persister:
                persister.log_command(command)
            return Integer(value)
        except TypeError:
            return Error("ERR value is not an integer or out of range")
    return Error("ERR wrong number of arguments for 'incr' command")


def _handle_decr(command, datastore, persister):
    if len(command) == 2:
        key = command[1].data.decode()
        try:
            value = datastore.decr(key)
            if persister:
                persister.log_command(command)
            return Integer(value)
        except TypeError:
            return Error("ERR value is not an integer or out of range")
    return Error("ERR wrong number of arguments for 'decr' command")


def _handle_lpush(command, datastore, persister):
    if len(command) >= 2:
        count = 0
        key = command[1].data.decode()

        try:
            for c in command[2:]:
                item = c.data.decode()
                count = datastore.prepend(key, item)
            if persister:
                persister.log_command(command)
            return Integer(count)
        except TypeError:
            return Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
    return Error("ERR wrong number of arguments for 'lpush' command")


def _handle_lrange(command, datastore):
    if len(command) == 4:
        key = command[1].data.decode()
        start = int(command[2].data.decode())
        stop = int(command[3].data.decode())

        try:
            items = datastore.lrange(key, start, stop)
            return Array([BulkString(i) for i in items])
        except TypeError:
            return Error(
                "WRONGTYPE Operatino against a key holding the wrong kind of value"
            )

    return Error("ERR wrong number of arguments for 'lrange' command")


def _handle_rpush(command, datastore, persister):
    if len(command) >= 2:
        count = 0
        key = command[1].data.decode()

        try:
            for c in command[2:]:
                item = c.data.decode()
                count = datastore.append(key, item)
            if persister:
                persister.log_command(command)
            return Integer(count)
        except TypeError:
            return Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
    return Error("ERR wrong number of arguments for 'rpush' command")


def _handle_unrecognised_command(command, *args):
    args = " ".join((f"'{c.data.decode()}'" for c in command[1:]))
    return Error(
        f"ERR unknown command '{command[0].data.decode()}', with args beginning with: {args}"
    )


def handle_command(command, datastore, persister):
    match command[0].data.decode().upper():
        case "ECHO":
            return _handle_echo(command)

        case "PING":
            return _handle_ping(command)

        case "SET":
            return _handle_set(command, datastore, persister)
        case "GET":
            return _handle_get(command, datastore)
        case "EXISTS":
            return _handle_exists(command, datastore)
        case "DEL":
            return _handle_del(command, datastore, persister)
        case "INCR":
            return _handle_incr(command, datastore, persister)
        case "DECR":
            return _handle_decr(command, datastore, persister)
        case "LPUSH":
            return _handle_lpush(command, datastore, persister)
        case "RPUSH":
            return _handle_rpush(command, datastore, persister)
        case "LRANGE":
            return _handle_lrange(command, datastore)
    return _handle_unrecognised_command(command)
