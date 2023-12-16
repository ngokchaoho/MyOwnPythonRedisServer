import argparse

from pyredis.server import Server
import logging


REDIS_DEFAULT_PORT = 6379


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
    main(args)
