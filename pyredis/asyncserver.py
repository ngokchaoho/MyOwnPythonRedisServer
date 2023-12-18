import asyncio

from pyredis.commands import handle_command
from pyredis.protocol import encode_message, extract_frame_from_buffer


class RedisServerProtocol(asyncio.Protocol):
    def __init__(self, datastore):
        self.buffer = bytearray()
        self._datastore = datastore

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        if not data:
            self.transport.close()

        self.buffer.extend(data)

        frame, frame_size = extract_frame_from_buffer(self.buffer)

        if frame:
            self.buffer = self.buffer[frame_size:]
            result = handle_command(frame, self._datastore)
            self.transport.write(encode_message(result))
