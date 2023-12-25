class AppendOnlyPersister:
    def __init__(self, filename):
        self._filename = filename
        self._file = open(filename, mode="ab", buffering=0)

    def log_command(self, command):
        self._file.write(f"*{len(command)}\r\n".encode())

        for item in command:
            self._file.write(item.resp_encode())
