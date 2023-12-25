import argparse
import asyncio
import trio
import logging
import threading
from time import sleep

from pyredis.server import Server
from pyredis.asyncserver import RedisServerProtocol
from pyredis.trioserver import TrioServer
from pyredis.datastore import DataStore
from pyredis.persistence import AppendOnlyPersister


REDIS_DEFAULT_PORT = 6379
log = logging.getLogger("pyredis")


def check_expiry_task(datastore):
    while True:
        datastore.remove_expired_keys()
        sleep(1)


async def acheck_expiry_task(datastore):
    while True:
        datastore.remove_expired_keys()
        await asyncio.sleep(1)


async def amain(args):
    log.info(f"Starting Pyredis on port: {args.port}")

    datastore = DataStore()

    loop = asyncio.get_running_loop()

    loop.create_task(acheck_expiry_task(datastore))

    persister = AppendOnlyPersister("ccdb.aof")

    server = await loop.create_server(
        lambda: RedisServerProtocol(datastore, persister), "127.0.0.1", args.port
    )

    async with server:
        await server.serve_forever()


async def tmain(args):
    server = TrioServer(args.port)
    await server.run()


def main(args):
    log.info(f"Starting PyRedis on port: {args.port}")

    datastore = DataStore()

    expiration_monitor = threading.Thread(target=check_expiry_task, args=(datastore,))
    expiration_monitor.start()

    server = Server(args.port, datastore)
    server.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple PyRedis server")
    parser.add_argument(
        "--port",
        metavar="p",
        type=int,
        nargs="?",
        help="Redis server listening port",
        default=REDIS_DEFAULT_PORT,
    )
    parser.add_argument("--asyncio", action=argparse.BooleanOptionalAction)
    parser.add_argument("--trio", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    if args.asyncio:
        log.info("Using AsyncIO RedisServerProtocol")
        asyncio.run(amain(args))
    elif args.trio:
        log.info("Using Trio Stream API")
        trio.run(tmain, args)
    else:
        log.info("Using threading module for multi-threading")
        main(args)
