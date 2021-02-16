# -*- coding: utf-8 -*-

#
# Copyright © 2021 Malek Hadj-Ali
# All rights reserved.
#
# This file is part of mood.
#
# mood is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3
# as published by the Free Software Foundation.
#
# mood is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with mood.  If not, see <http://www.gnu.org/licenses/>.
#


from collections import deque

from mood.event import fatal, EV_READ

from .connections import Connection, Overwatch
from .loops import watcher, ServerLoop, ClientLoop
from .pack import encode, size, unpack
from .sockets import ServerSocket, ClientSocket


class CriticalError(Exception):
    pass

class RequestError(RuntimeError):
    pass


# public decorator -------------------------------------------------------------

def public(func):
    func.__public__ = True
    return func


# ------------------------------------------------------------------------------
# Server

class IPPCClient(Connection):

    def __init__(self, handler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._handler = handler
        self.wait()

    def __on_request__(self, buf):
        self.write(self._handler(buf), self.wait)

    def __on_size__(self, buf):
        self.read(size(buf), self.__on_request__)

    def __on_len__(self, buf):
        self.read(buf[0], self.__on_size__)

    def wait(self):
        self.read(1, self.__on_len__)


class Server(ServerLoop):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._clients = deque()
        self._methods = {}
        for item in kwargs.items():
            self._methods.update(self.__methods__(*item))

    def __methods__(self, key, value):
        for name in dir(value):
            if (
                (not name.startswith("_")) and
                (callable(_value_ := getattr(value, name))) and
                (getattr(_value_, "__public__", False))
            ):
                _key_ = f"{key}.{name}" if key != "__main__" else name
                yield _key_, _value_

    def __on_close__(self, client):
        self._clients.remove(client)

    def __on_request__(self, buf):
        try:
            try:
                name, args, kwargs = unpack(buf)
                try:
                    result = self._methods[name](*args, **kwargs)
                except KeyError:
                    raise AttributeError(f"no method '{name}'") from None
            except CriticalError as err:
                raise err
            except Exception as err:
                self._logger.exception(f"{self}: error processing request")
                result = err
            return encode(result)
        except Exception:
            self.__on_error__("critical error processing request")

    def __on_accept__(self, *args): # watcher callback
        try:
            while True:
                try:
                    socket = self._socket.accept()
                except BlockingIOError:
                    break
                else:
                    self._clients.append(
                        IPPCClient(
                            self.__on_request__,
                            socket, self._loop, self._logger,
                            on_close=self.__on_close__
                        )
                    )
        except Exception:
            self.__on_error__("critical error accepting a connection")

    def setup(self, name):
        self._socket = ServerSocket(name)
        return (self._loop.io(self._socket, EV_READ, self.__on_accept__),)

    def stopping(self):
        while self._clients:
            self._clients.pop().close(False)
        self._socket.close()


# ------------------------------------------------------------------------------
# Client

class IPPCAttribute(object):

    def __init__(self, handler, name):
        self._handler = handler
        self._name = name

    def __getattr__(self, name):
        return IPPCAttribute(self._handler, f"{self._name}.{name}")

    def __call__(self, *args, **kwargs):
        return self._handler(self._name, args, kwargs)


class IPPCConnection(Overwatch):

    def __init__(self, name, *args, **kwargs):
        super().__init__(ClientSocket(name), *args, **kwargs)

    def __on_result__(self, buf):
        try:
            self._result = unpack(buf)
        except Exception as err:
            self._result = err
        finally:
            self.__unblock__()

    def __on_size__(self, buf):
        self.read(size(buf), self.__on_result__)

    def __on_len__(self, buf):
        self.read(buf[0], self.__on_size__)

    def wait(self):
        self.read(1, self.__on_len__)

    def __on_request__(self, name, args, kwargs):
        self._result = RequestError()
        self.write(encode((name, args, kwargs)), self.wait)
        self.__block__()
        if isinstance(self._result, Exception):
            raise self._result
        return self._result

    def __getattr__(self, name):
        return IPPCAttribute(self.__on_request__, name)


class Client(ClientLoop):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ippc = None

    def setup(self, name):
        self.ippc = IPPCConnection(
            name, self._loop, self._logger, on_close=self.stop
        )
        return ()

    def stopping(self):
        if self.ippc:
            self.ippc.close()

