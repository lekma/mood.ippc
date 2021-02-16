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


import builtins
from logging import getLogger
from collections import deque
from os import getpid
from signal import pthread_sigmask, SIG_UNBLOCK, SIGINT, SIGTERM

from mood.event import (
    loop, Loop, EVFLAG_AUTO, EVFLAG_NOSIGMASK, EVBREAK_ALL, EV_MAXPRI
)

from .pack import register


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


# watcher decorator ------------------------------------------------------------

def watcher(wtype, *wargs, **wkwargs):
    def decorator(func):
        func.__watcher__ = (wtype, wargs, wkwargs)
        return func
    return decorator


# ------------------------------------------------------------------------------
# __SignalLoop__

class __SignalLoop__(object):

    def __init__(self, logger, flags=EVFLAG_AUTO):
        self._loop = self.__ctor__(flags=(flags | EVFLAG_NOSIGMASK))
        self._logger = logger
        self._watchers = deque()
        self._stopping = False
        self._pid = getpid()

    def __repr__(self):
        cls = self.__class__
        return f"<{cls.__module__}.{cls.__name__} pid={self._pid}>"

    def __on_error__(self, message, exc_info=True):
        try:
            suffix = " -> stopping" if not self._stopping else ""
            self._logger.critical(f"{self}: {message}{suffix}", exc_info=exc_info)
        finally:
            self.stop() # stop on error

    def __register__(self, *args):
        self._watchers.extend(args)

    def __watchers__(self):
        for name in dir(self):
            if (
                (not name.startswith("_")) and
                (callable(cb := getattr(self, name))) and
                (watcher := getattr(cb, "__watcher__", None))
            ):
                wtype, wargs, wkwargs = watcher
                yield getattr(self._loop, wtype)(
                    *wargs, cb,
                    wkwargs.get("data", None),
                    wkwargs.get("priority", 0)
                )

    def __starter__(self, watcher, revents): # watcher callback
        watcher.stop()
        self._watchers.remove(watcher)
        try:
            self.starting()
        except Exception:
            self.__on_error__("error while starting")
        else:
            self._logger.info(f"{self}: started")

    def __setup__(self, *args, signals={SIGINT, SIGTERM}):
        pthread_sigmask(SIG_UNBLOCK, signals)
        self.__register__(
            self._loop.prepare(self.__starter__),
            *(
                self._loop.signal(signal, self.stop, priority=EV_MAXPRI)
                for signal in signals
            ),
            *self.__watchers__(),
            *args
        )

    # --------------------------------------------------------------------------

    @property
    def stopped(self):
        return self._loop.depth == 0

    def stop(self, *args): # watcher callback
        if not self.stopped and not self._stopping:
            self._stopping = True
            self._logger.info(f"{self}: stopping...")
            while self._watchers:
                self._watchers.pop().stop()
            try:
                self.stopping()
            except Exception:
                self.__on_error__("error while stopping")
            finally:
                self._loop.stop(EVBREAK_ALL)
                self._logger.info(f"{self}: stopped")
                self._stopping = False

    def start(self, *args, **kwargs):
        if self.stopped:
            self._logger.info(f"{self}: starting...")
            self.__setup__(*self.setup(*args, **kwargs))
            for watcher in self._watchers:
                watcher.start()
            while self._loop.start():
                pass

    def setup(self, *args, **kwargs):
        return ()

    def starting(self):
        pass

    def stopping(self):
        pass


# ------------------------------------------------------------------------------
# __BaseLoop__

class __BaseLoop__(__SignalLoop__):

    def __init__(self, **kwargs):
        register_errors()
        register_types(*kwargs.pop("types", ()))
        super().__init__(
            kwargs.pop("logger", getLogger(__name__)),
            flags=kwargs.pop("flags", EVFLAG_AUTO)
        )


# ------------------------------------------------------------------------------
# ServerLoop

class ServerLoop(__BaseLoop__):

    __ctor__ = loop


# ------------------------------------------------------------------------------
# ClientLoop

class ClientLoop(__BaseLoop__):

    __ctor__ = Loop

