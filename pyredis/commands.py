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


def _handle_set(command, datastore):
    if len(command) >= 3:
        key = command[1].data.decode()
        value = command[2].data.decode()
        datastore[key] = value
        return SimpleString("OK")
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


def _handle_unrecognised_command(command, *args):
    args = " ".join((f"'{c.data.decode()}'" for c in command[1:]))
    return Error(
        f"ERR unknown command '{command[0].data.decode()}', with args beginning with: {args}"
    )


def handle_command(command, datastore):
    match command[0].data.decode().upper():
        case "ECHO":
            return _handle_echo(command)

        case "PING":
            return _handle_ping(command)

        case "SET":
            return _handle_set(command, datastore)

        case "GET":
            return _handle_get(command, datastore)

    return _handle_unrecognised_command(command)
