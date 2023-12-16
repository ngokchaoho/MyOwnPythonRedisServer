from pyredis.types import (
    Array,
    BulkString,
    Error,
    Integer,
    SimpleString,
)


_MSG_SEPARATOR = b"\r\n"
_MSG_SEPARATOR_SIZE = len(_MSG_SEPARATOR)


def extract_frame_from_buffer(buffer):
    separator = buffer.find(_MSG_SEPARATOR)

    if separator == -1:
        return None, 0
    else:
        paylood = buffer[1:separator].decode()
    match chr(buffer[0]):
        case "+":
            return SimpleString(paylood), separator + _MSG_SEPARATOR_SIZE

        case "-":
            return Error(buffer[1:separator].decode()), separator + _MSG_SEPARATOR_SIZE

        case ":":
            return Integer(int(buffer[1:separator])), separator + _MSG_SEPARATOR_SIZE

        case "$":
            data_size = int(paylood)
            # NULL bulk String
            if data_size == -1:
                return None, 5
            content_saparator = buffer.find(_MSG_SEPARATOR, separator + 1)
            if (
                data_size >= 0
                and content_saparator != -1
                and separator + _MSG_SEPARATOR_SIZE + data_size + _MSG_SEPARATOR_SIZE
            ):
                return (
                    BulkString(data=buffer[separator + 2 : separator + 2 + data_size]),
                    data_size + 2 + separator + 2,
                )

            # Everything else is considered illegal
            return None, 0

        case "*":
            if separator != -1:
                size = int(buffer[1:separator].decode())
                if size == -1:
                    return Array(None), separator + _MSG_SEPARATOR_SIZE
                if size == 0:
                    return Array([]), separator + _MSG_SEPARATOR_SIZE

                array_builder = Array([])

                total_frame_size = separator + 2
                for _ in range(size):
                    frame, frame_size = extract_frame_from_buffer(
                        buffer[separator + 2 :]
                    )
                    if frame and frame_size:
                        array_builder.data.append(frame)
                        separator += frame_size
                        total_frame_size += frame_size
                    else:
                        return None, 0

                return array_builder, total_frame_size
    return None, 0


def encode_message(message):
    return message.resp_encode()