import argparse
import asyncio

from pyredis.server import Server
from pyredis.asyncserver import RedisServerProtocol
import logging


REDIS_DEFAULT_PORT = 6379


async def amain(args):
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: RedisServerProtocol(), "127.0.0.1", args.port
    )

    async with server:
        await server.serve_forever()


def main(args):
    print(f"Starting PyRedis on port: {args.port}")

    server = Server(args.port)
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
        logging.info("Using AsyncIO RedisServerProtocol")
        asyncio.run(amain(args))
    else:
        logging.info("Using threading module for multi-threading")
        main(args)
