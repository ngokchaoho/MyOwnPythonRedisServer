import socket
import logging

from pyredis.protocol import extract_frame_from_buffer, encode_message
from pyredis.commands import handle_command
from pyredis.datastore import DataStore

RECV_SIZE = 2048
log = logging.getLogger("pyredis")


class Server:
    def __init__(self, port) -> None:
        self.port = port
        self._running = False
        self._datastore = DataStore()

    def run(self):
        self._running = True

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self._server_socket = server_socket
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # bind
            server_socket.bind(("localhost", self.port))
            server_socket.listen()

            while self._running:
                client_socket, _ = server_socket.accept()
                log.info("Accepted one client")
                self.handle_client_connection(client_socket, self._datastore)

    def handle_client_connection(self, client_socket, datastore):
        buffer = bytearray()
        try:
            while True:
                data = client_socket.recv(RECV_SIZE)
                log.info("Received data from client")
                if not data:
                    break
                buffer.extend(data)
                frame, frame_size = extract_frame_from_buffer(buffer)
                log.info("Extracted one frame from received data")
                if frame:
                    buffer = buffer[frame_size:]
                    log.info("Processing one frame")
                    result = handle_command(frame, datastore)
                    client_socket.send(encode_message(result))

        finally:
            client_socket.close()

    def stop(self):
        self._running = False
