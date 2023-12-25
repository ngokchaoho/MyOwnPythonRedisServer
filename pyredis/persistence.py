from pyredis.commands import handle_command
from pyredis.protocol import extract_frame_from_buffer


class AppendOnlyPersister:
    def __init__(self, filename):
        self._filename = filename
        self._file = open(filename, mode="ab", buffering=0)

    def log_command(self, command):
        self._file.write(f"*{len(command)}\r\n".encode())

        for item in command:
            self._file.write(item.resp_encode())

    @staticmethod
    def restore_from_file(filename=None, database=None):
        buffer = bytearray()
        with open(filename, "rb") as f:
            while True:
                data = f.read(1024)
                if data:
                    buffer.extend(data)
                else:
                    break
                while True:
                    frame, frame_size = extract_frame_from_buffer(buffer)

                    if frame:
                        buffer = buffer[frame_size:]
                        handle_command(frame, database, None)
                    else:
                        break
        return True
