#!/root/anaconda3/envs/redis/bin/python
from pyredis.protocol import encode_message, extract_frame_from_buffer
from pyredis.types import Array, BulkString

import socket
import argparse

DEFAULT_PORT = 6379
DEFAULT_SERVER = "127.0.0.1"
RECV_SIZE = 1024


def encode_command(command):
    return Array([BulkString(p) for p in command.split()])


def main(args):
    server = args.server
    port = args.port
    with socket.socket() as client_socket:
        client_socket.connect((server, port))

        buffer = bytearray()

        while True:
            command = input(f"{server}:{port}> ")

            if command == "quit":
                break
            else:
                encoded_message = encode_message(encode_command(command))
                client_socket.send(encoded_message)

                while True:
                    data = client_socket.recv(RECV_SIZE)
                    buffer.extend(data)

                    frame, frame_size = extract_frame_from_buffer(buffer)

                    if frame:
                        buffer = buffer[frame_size:]
                        if isinstance(frame, Array):
                            for count, item in enumerate(frame.data):
                                print(f"{count +1} {item}")
                        else:
                            print(frame)
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple Redis Client")
    parser.add_argument(
        "--server",
        metavar="s",
        type=str,
        nargs="?",
        help="Redis server ip address",
        default=DEFAULT_SERVER,
    )
    parser.add_argument(
        "--port",
        metavar="p",
        type=int,
        nargs="?",
        help="Redis server listening port",
        default=DEFAULT_PORT,
    )
    args = parser.parse_args()
    main(args)
