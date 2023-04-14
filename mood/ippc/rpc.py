# -*- coding: utf-8 -*-

#
# Copyright © 2022 Malek Hadj-Ali
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


__all__ = [
    "fatal", "public", "watcher",
    "CriticalError", "RequestError",
    "ClientLoop", "ServerLoop"
]


import builtins
from collections import deque
from logging import getLogger
from os import getpid
from signal import pthread_sigmask, SIG_UNBLOCK, SIGINT, SIGTERM

from mood.event import (
    fatal, loop, Loop,
    EV_MAXPRI, EV_READ, EVBREAK_ALL, EVFLAG_AUTO, EVFLAG_NOSIGMASK
)

from .connection import Connection
from .pack import encode, register, size, unpack
from .sockets import ClientSocket, ServerSocket


class CriticalError(Exception):
    pass

class RequestError(RuntimeError):
    pass


# public decorator -------------------------------------------------------------

def public(func):
    func.__public__ = True
    return func


# watcher decorator ------------------------------------------------------------

def watcher(wtype, *wargs, **wkwargs):
    def decorator(func):
        func.__watcher__ = (wtype, wargs, wkwargs)
        return func
    return decorator


# helpers ----------------------------------------------------------------------

def __is_error__(obj):
    return (
        isinstance(obj, type) and
        issubclass(obj, Exception) and
        not issubclass(obj, Warning)
    )

def register_types(*args):
    for arg in args:
        register(arg)

def register_errors():
    register_types(*(v for v in vars(builtins).values() if __is_error__(v)))


# ------------------------------------------------------------------------------
# Overwatch

class Overwatch(Connection):

    def __setup__(self, socket, loop, **kwargs):
        self._loop_ = Loop(
            flags=(kwargs.pop("flags", EVFLAG_AUTO) | EVFLAG_NOSIGMASK)
        )
        super().__setup__(socket, self._loop_, **kwargs)
        # overwatch
        self._overwatch_ = loop.io(socket, EV_READ, self.__on_read__)
        self._overwatch_.start()

    def __stop__(self):
        self._loop_.stop(EVBREAK_ALL)
        self._overwatch_.stop()
        super().__stop__()

    def __cleanup__(self):
        self._overwatch_ = None # break cycles
        super().__cleanup__()

    def __block__(self):
        self._overwatch_.stop()
        self._loop_.start()

    def __unblock__(self):
        self._loop_.stop(EVBREAK_ALL)
        self._overwatch_.start()


# ------------------------------------------------------------------------------
# BaseLoop

class BaseLoop(object):

    def __init__(self, **kwargs):
        register_errors()
        register_types(*kwargs.pop("types", ()))
        self._loop_ = self.__loop__(
            flags=(kwargs.pop("flags", EVFLAG_AUTO) | EVFLAG_NOSIGMASK),
            callback=kwargs.pop("callback", None),
            data=kwargs.pop("data", None),
            io_interval=kwargs.pop("io_interval", 0.0),
            timeout_interval=kwargs.pop("timeout_interval", 0.0)
        )
        self.logger = kwargs.pop("logger", getLogger(__name__))
        self._watchers_ = deque()
        self._stopping_ = False
        self._pid_ = getpid()

    def __repr__(self):
        cls = self.__class__
        return f"<{cls.__module__}.{cls.__name__} pid={self._pid_}>"

    def __on_error__(self, message, exc_info=True):
        try:
            suffix = " -> stopping" if not self._stopping_ else ""
            self.logger.critical(
                f"{self}: {message}{suffix}", exc_info=exc_info
            )
        finally:
            self.stop() # stop on error

    def __watchers__(self):
        for name in dir(self):
            if (
                (not name.startswith("_")) and
                (callable(wcallback := getattr(self, name))) and
                (watcher := getattr(wcallback, "__watcher__", None))
            ):
                wtype, wargs, wkwargs = watcher
                yield getattr(self._loop_, wtype)(
                    *wargs,
                    wcallback,
                    wkwargs.get("data", None),
                    wkwargs.get("priority", 0)
                )

    def __starter__(self, watcher, revents): # watcher callback
        watcher.stop()
        self._watchers_.remove(watcher)
        try:
            self.starting()
        except Exception:
            self.__on_error__("error while starting")
        else:
            self.logger.info(f"{self}: started")

    def __setup__(self, *args, signals={SIGINT, SIGTERM}, **kwargs):
        self.register(self._loop_.prepare(self.__starter__))
        pthread_sigmask(SIG_UNBLOCK, signals)
        for signal in signals:
            self.register(
                self._loop_.signal(signal, self.stop, priority=EV_MAXPRI)
            )
        self.register(*self.__watchers__())
        self.setup(*args, **kwargs)

    # --------------------------------------------------------------------------

    def register(self, *args):
        self._watchers_.extend(args)

    @property
    def stopped(self):
        return self._loop_.depth == 0

    def start(self, *args, **kwargs):
        if self.stopped:
            self.logger.info(f"{self}: starting...")
            self.__setup__(*args, **kwargs)
            for watcher in self._watchers_:
                watcher.start()
            while self._loop_.start():
                pass

    def stop(self, *args): # watcher callback
        if not self.stopped and not self._stopping_:
            self._stopping_ = True
            self.logger.info(f"{self}: stopping...")
            while self._watchers_:
                self._watchers_.pop().stop()
            try:
                self.stopping()
            except Exception:
                self.__on_error__("error while stopping")
            finally:
                self._loop_.stop(EVBREAK_ALL)
                self.logger.info(f"{self}: stopped")
                self._stopping_ = False

    # --------------------------------------------------------------------------

    def setup(self, *args, **kwargs):
        pass

    def starting(self):
        pass

    def stopping(self):
        pass


# ------------------------------------------------------------------------------
# Server

class Peer(Connection):

    def __init__(self, handler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._handler_ = handler
        self.wait()

    def __on_request__(self, buf):
        self.write(self._handler_(buf), self.wait)

    def __on_size__(self, buf):
        self.read(size(buf), self.__on_request__)

    def __on_len__(self, buf):
        self.read(buf[0], self.__on_size__)

    def wait(self):
        self.read(1, self.__on_len__)


class ServerLoop(BaseLoop):

    __loop__ = loop

    @staticmethod
    def __methods__(value, key=None):
        for name in dir(value):
            if (
                (not name.startswith("_")) and
                (callable(method := getattr(value, name))) and
                (getattr(method, "__public__", False))
            ):
                yield (f"{key}.{name}" if key else name, method)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._methods_ = {}
        self._methods_.update(self.__methods__(self))
        for key, value in kwargs.items():
            self._methods_.update(self.__methods__(value, key))
        self._peers_ = deque()

    def __on_close__(self, peer):
        self._peers_.remove(peer)

    def __on_request__(self, buf):
        try:
            try:
                name, args, kwargs = unpack(buf)
                try:
                    result = self._methods_[name](*args, **kwargs)
                except KeyError:
                    raise AttributeError(f"no method '{name}'") from None
            except CriticalError as err:
                raise err
            except Exception as err:
                self.logger.exception(f"{self}: error processing request")
                result = err
            return encode(result)
        except Exception:
            self.__on_error__("critical error processing request")

    def __on_accept__(self, *args): # watcher callback
        try:
            while True:
                try:
                    socket = self._socket_.accept()
                except BlockingIOError:
                    break
                else:
                    self._peers_.append(
                        Peer(
                            self.__on_request__,
                            socket,
                            self._loop_,
                            logger=self.logger,
                            on_close=self.__on_close__
                        )
                    )
        except Exception:
            self.__on_error__("critical error accepting a connection")

    def setup(self, name):
        self._socket_ = ServerSocket(name)
        self.register(
            self._loop_.io(self._socket_, EV_READ, self.__on_accept__)
        )

    def stopping(self):
        while self._peers_:
            self._peers_.pop().close(False)
        self._socket_.close()


# ------------------------------------------------------------------------------
# Client

class Attribute(object):

    def __init__(self, handler, name):
        self._handler_ = handler
        self._name_ = name

    def __getattr__(self, name):
        return Attribute(self._handler_, f"{self._name_}.{name}")

    def __call__(self, *args, **kwargs):
        return self._handler_(self._name_, args, kwargs)


class Client(Overwatch):

    def __init__(self, name, *args, **kwargs):
        super().__init__(ClientSocket(name), *args, **kwargs)

    def __getattr__(self, name):
        return Attribute(self.__on_request__, name)

    def __on_result__(self, buf):
        try:
            self._result_ = unpack(buf)
        except Exception as err:
            self._result_ = err
        finally:
            self.__unblock__()

    def __on_size__(self, buf):
        self.read(size(buf), self.__on_result__)

    def __on_len__(self, buf):
        self.read(buf[0], self.__on_size__)

    def wait(self):
        self.read(1, self.__on_len__)

    def __on_request__(self, name, args, kwargs):
        self._result_ = RequestError()
        self.write(encode((name, args, kwargs)), self.wait)
        self.__block__()
        if isinstance(self._result_, Exception):
            raise self._result_
        return self._result_


class ClientLoop(BaseLoop):

    __loop__ = Loop

    def __init__(self, **kwargs):
        self._flags_ = kwargs.pop("flags", EVFLAG_AUTO)
        super().__init__(**kwargs)
        self.client = None

    def setup(self, name):
        self.client = Client(
            name,
            self._loop_,
            logger=self.logger,
            on_close=self.stop,
            flags=self._flags_
        )

    def stopping(self):
        if self.client:
            self.client.close()
