from trio import SocketListener, serve_tcp, SocketStream
import logging
import trio

from pyredis.protocol import extract_frame_from_buffer, encode_message
from pyredis.commands import handle_command
from pyredis.datastore import DataStore

RECV_SIZE = 2048
log = logging.getLogger("pyredis")


class TrioServer:
    def __init__(self, port) -> None:
        self.port = port
        self._running = False
        self._datastore = DataStore()

    async def run(self):
        self._running = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(serve_tcp, self.handle_client_connection, self.port)

    async def handle_client_connection(self, client_stream: SocketStream):
        buffer = bytearray()
        try:
            while True:
                data = await client_stream.receive_some(RECV_SIZE)
                log.info("Received data from client")
                if not data:
                    log.info("Readched EOF")
                    break
                buffer.extend(data)
                frame, frame_size = extract_frame_from_buffer(buffer)
                if frame:
                    buffer = buffer[frame_size:]
                    result = handle_command(frame, self._datastore)
                    await client_stream.send_all(encode_message(result))

        finally:
            log.info("Attempt to close stream")
            await client_stream.aclose()

    def stop(self):
        self._running = False
