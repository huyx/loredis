# -*- coding: utf-8 -*-
import shlex


class ProtocolError(Exception):
    pass

class ReplyError(Exception):
    pass


def check_error_class(error_class):
    try:
        raise error_class('error')
    except TypeError:
        raise
    except Exception:
        pass


class Reader:
    _buffer = b''
    _accept_inline_command = False

    def __init__(self, encoding=None, protocolError=ProtocolError, replyError=ReplyError):
        check_error_class(protocolError)
        check_error_class(replyError)

        self.encoding = encoding
        self.protocolError = protocolError
        self.replyError = replyError

    def gets(self):
        result, self._buffer = self._gets(self._buffer)
        return result

    def _decode(self, data):
        if self.encoding:
            return data.decode(self.encoding)
        return data

    def _gets(self, buff):
        if not buff:
            return False, buff

        orig_buff = buff
        line, eol, buff = buff.partition(b'\r\n')

        if not eol:
            return False, buff

        flag = line[0:1]

        if flag == b'+':
            result = line[1:]
            return self._decode(result), buff
        elif flag == b'-':
            result = self.replyError(line[1:].decode())
            return result, buff
        elif flag == b':':
            result = int(line[1:])
            return result, buff
        elif flag == b'$':
            strlen = int(line[1:])
            if len(buff) >= strlen + 2:
                result = buff[:strlen]
            if buff[strlen:strlen+2] != b'\r\n':
                raise self.protocolError('error')
            buff = buff[strlen+2:]
            return self._decode(result), buff
        elif flag == b'*':
            count = int(line[1:])
            if count == -1:
                return None, buff
            array = []
            for _ in range(count):
                result, buff = self._gets(buff)
                if result == False:
                    return False, orig_buff
                array.append(self._decode(result))
            else:
                return array, buff
        elif self._accept_inline_command:
            return shlex.split(line.decode()), buff
        else:
            raise self.protocolError('protocol error')

    def feed(self, data, start=0, length=0):
        if start+length > len(data):
            raise ValueError

        if length:
            self._buffer += data[start:start+length]
        else:
            self._buffer += data[start:]


class ServerReader(Reader):
    _accept_inline_command = True


def _encode_int(i):
    return str(i).encode('ascii')

def INT(i):
    return b':' + _encode_int(i) + b'\r\n'

def SIMPLE_STRING(s):
    return b'+' + s + b'\r\n'

def ERROR(errmsg):
    return b'-' + errmsg + b'\r\n'

def BULK_STRING(s):
    if not s:
        return b'$-1\r\n'
    return b''.join((b'$', _encode_int(len(s)), b'\r\n', s, b'\r\n'))

def ARRAY(a):
    if not a:
        return b'*-1\r\n'
    l = [b'*', _encode_int(len(a)), b'\r\n']
    l += a
    return b''.join(l)

def BULK_STRING_ARRAY(args):
    return ARRAY([BULK_STRING(arg) for arg in args])

build_command = BULK_STRING_ARRAY
