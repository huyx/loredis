# -*- coding: utf-8 -*-
from . import loredis
import pytest
import sys

reader = None

def setup_function(f):
    global reader
    reader = loredis.Reader()

def assertEquals(a, b):
    assert a == b

def test_nothing():
    assert False == reader.gets()

def test_error_when_feeding_non_string():
    with pytest.raises(TypeError):
        reader.feed(1)

def test_protocol_error():
    reader.feed(b"x\r\n")
    with pytest.raises(loredis.ProtocolError):
        reader.gets()

def test_protocol_error_with_custom_class():
    reader = loredis.Reader(protocolError=RuntimeError)
    reader.feed(b"x\r\n")
    with pytest.raises(RuntimeError):
        reader.gets()

def test_protocol_error_with_custom_callable():
    class CustomException(Exception):
        pass

    reader = loredis.Reader(protocolError=lambda e: CustomException(e))
    reader.feed(b"x\r\n")
    with pytest.raises(CustomException):
        reader.gets()

def test_fail_with_wrong_protocol_error_class():
    with pytest.raises(TypeError):
        loredis.Reader(protocolError="wrong")

def test_error_string():
    reader.feed(b"-error\r\n")
    error = reader.gets()

    assert loredis.ReplyError == type(error)
    assert ("error",) == error.args

def test_error_string_with_custom_class():
    reader = loredis.Reader(replyError=RuntimeError)
    reader.feed(b"-error\r\n")
    error = reader.gets()

    assert RuntimeError == type(error)
    assert ("error",) == error.args

def test_error_string_with_custom_callable():
    class CustomException(Exception):
        pass

    reader = loredis.Reader(replyError=lambda e: CustomException(e))
    reader.feed(b"-error\r\n")
    error = reader.gets()

    assert CustomException == type(error)
    assert ("error",) == error.args

def test_fail_with_wrong_reply_error_class():
    with pytest.raises(TypeError):
        loredis.Reader(replyError="wrong")

def test_errors_in_nested_multi_bulk():
    reader.feed(b"*2\r\n-err0\r\n-err1\r\n")

    for r, error in zip(("err0", "err1"), reader.gets()):
        assert loredis.ReplyError == type(error)
        assert (r,) == error.args

def test_integer():
    value = 2 ** 63 - 1    # Largest 64-bit signed integer
    reader.feed((":%d\r\n" % value).encode("ascii"))
    assert value == reader.gets()

def test_status_string():
    reader.feed(b"+ok\r\n")
    assert b"ok" == reader.gets()

def test_empty_bulk_string():
    reader.feed(b"$0\r\n\r\n")
    assert b"" == reader.gets()

def test_bulk_string():
    reader.feed(b"$5\r\nhello\r\n")
    assert b"hello" == reader.gets()

def test_bulk_string_without_encoding():
    snowman = b"\xe2\x98\x83"
    reader.feed(b"$3\r\n" + snowman + b"\r\n")
    assert snowman == reader.gets()

def test_bulk_string_with_encoding():
    snowman = b"\xe2\x98\x83"
    reader = loredis.Reader(encoding="utf-8")
    reader.feed(b"$3\r\n" + snowman + b"\r\n")
    assert snowman.decode("utf-8") == reader.gets()

# !!! not supperted !!!
# def test_bulk_string_with_other_encoding():
#     snowman = b"\xe2\x98\x83"
#     reader = loredis.Reader(encoding="utf-32")
#     reader.feed(b"$3\r\n" + snowman + b"\r\n")
#     assert snowman == reader.gets()

# !!! not supperted !!!
# def test_bulk_string_with_invalid_encoding():
#     reader = loredis.Reader(encoding="unknown")
#     reader.feed(b"$5\r\nhello\r\n")
#     assert LookupError == reader.gets()

def test_null_multi_bulk():
    reader.feed(b"*-1\r\n")
    assert None == reader.gets()

def test_empty_multi_bulk():
    reader.feed(b"*0\r\n")
    assert [] == reader.gets()

def test_multi_bulk():
    reader.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
    assert [b"hello", b"world"] == reader.gets()

# def test_multi_bulk_with_invalid_encoding_and_partial_reply():
#     reader = loredis.Reader(encoding="unknown")
#     reader.feed(b"*2\r\n$5\r\nhello\r\n")
#     assert False == reader.gets()
#     reader.feed(b":1\r\n")
#     while pytest.raises(LookupError):
#         reader.gets()

def test_nested_multi_bulk():
    reader.feed(b"*2\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n$1\r\n!\r\n")
    assert [[b"hello", b"world"], b"!"] == reader.gets()

def test_nested_multi_bulk_depth():
    reader.feed(b"*1\r\n*1\r\n*1\r\n*1\r\n$1\r\n!\r\n")
    assert [[[[b"!"]]]] == reader.gets()

def test_subclassable():
    class TestReader(loredis.Reader):
        def __init__(self, *args, **kwargs):
            loredis.Reader.__init__(self, *args, **kwargs)

    reader = TestReader()
    reader.feed(b"+ok\r\n")
    assert b"ok" == reader.gets()

def test_invalid_offset():
    data = b"+ok\r\n"
    with pytest.raises(ValueError):
        reader.feed(data, 6)

def test_invalid_length():
    data = b"+ok\r\n"
    with pytest.raises(ValueError):
        reader.feed(data, 0, 6)

def test_ok_offset():
    data = b"blah+ok\r\n"
    reader.feed(data, 4)
    assert b"ok" == reader.gets()

def test_ok_length():
    data = b"blah+ok\r\n"
    reader.feed(data, 4, len(data) - 4)
    assert b"ok" == reader.gets()

def test_feed_bytearray():
    if sys.hexversion >= 0x02060000:
        reader.feed(bytearray(b"+ok\r\n"))
        assert b"ok" == reader.gets()

def test_inline_ping():
    reader._accept_inline_command = True
    reader.feed(b"ping\r\n")
    assert ["ping"] == reader.gets()

def test_inline_set():
    reader._accept_inline_command = True
    reader.feed(b"set key value\r\n")
    assert ["set", "key", "value"] == reader.gets()

def test_inline_pipeline():
    reader._accept_inline_command = True
    reader.feed(b"ping\r\n")
    reader.feed(b"set key value\r\n")
    assert ["ping"] == reader.gets()
    assert ["set", "key", "value"] == reader.gets()

def test_INT():
    assert b':100\r\n' == loredis.INT(100)

def test_SIMPLE_STRING():
    assert b'+hello\r\n' == loredis.SIMPLE_STRING(b'hello')

def test_ERROR():
    assert b'-error\r\n' == loredis.ERROR(b'error')

def test_BULK_STRING():
    assert b'$5\r\nhello\r\n' == loredis.BULK_STRING(b'hello')

def test_ARRAY():
    assert b'*3\r\n+A\r\n+B\r\n+C\r\n' == loredis.ARRAY((
        loredis.SIMPLE_STRING(b'A'),
        loredis.SIMPLE_STRING(b'B'),
        loredis.SIMPLE_STRING(b'C')))

def test_build_command():
    assert b'*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n' == loredis.build_command([b'GET', b'FOO'])
