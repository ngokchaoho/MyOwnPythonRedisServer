
import pytest

from pyredis.protocol import extract_frame_from_buffer
from pyredis.types import (
    Array,
    BulkString,
    Error,
    Integer,
    SimpleString,
)


@pytest.mark.parametrize(
    "buffer, expected",
    [
        # Test invalid Message
        (b"PING\r\n", (None, 0)),
        # Test cases for Simple Strings
        (b"+Par", (None, 0)),
        (b"+OK\r\n", (SimpleString("OK"), 5)),
        (b"+OK\r\n+Next", (SimpleString("OK"), 5)),
        # Test cases for Errors
        (b"-Err", (None, 0)),
        (b"-Error Message\r\n", (Error("Error Message"), 16)),
        (b"-Error Message\r\n+Other", (Error("Error Message"), 16)),
        # Test cases for Integers
        (b":10", (None, 0)),
        (b":100\r\n", (Integer(100), 6)),
        (b":100\r\n+OK", (Integer(100), 6)),
        # Test cases for Bulk Strings
        (b"$5\r\nHel", (None, 0)),
        (b"$5\r\nHello\r\n", (BulkString(b"Hello"), 11)),
        (b"$12\r\nHello, World\r\n", (BulkString(b"Hello, World"), 19)),
        (b"$12\r\nHello\r\nWorld\r\n", (BulkString(b"Hello\r\nWorld"), 19)),
        (b"$0\r\n\r\n", (BulkString(b""), 6)),
        (b"$-1\r\n", (None, 5)),
        # Test case for Arrays
        (b"*0", (None, 0)),
        (b"*0\r\n", (Array([]), 4)),
        (b"*-1\r\n", (Array(None), 5)),
        (b"*2\r\n$5\r\nhello\r\n$5\r\n", (None, 0)),
        (
            b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            (Array([BulkString(b"hello"), BulkString(b"world")]), 26),
        ),
        (
            b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n+OK",
            (Array([BulkString(b"hello"), BulkString(b"world")]), 26),
        ),
        (b"*3\r\n:1\r\n:", (None, 0)),
        (
            b"*3\r\n:1\r\n:2\r\n:3\r\n",
            (Array([Integer(1), Integer(2), Integer(3)]), 16),
        ),
        (
            b"*3\r\n:1\r\n:2\r\n:3\r\n+OK",
            (Array([Integer(1), Integer(2), Integer(3)]), 16),
        ),
    ],
)
def test_read_frame(buffer, expected):
    actual = extract_frame_from_buffer(buffer)
    assert actual == expected