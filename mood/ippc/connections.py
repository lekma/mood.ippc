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


from logging import ERROR, DEBUG
from collections import deque

from mood.event import Loop, EVFLAG_NOSIGMASK, EV_READ, EV_WRITE, EVBREAK_ALL


# ------------------------------------------------------------------------------
# Connection

class Connection(object):

    def __setup__(self, socket, loop, logger, on_close=None):
        self._socket = socket
        self._logger = logger
        self._on_close = on_close
        self._closing = False
        # writer
        self._wtasks = deque()
        self._writer = loop.io(socket, EV_WRITE, self.__on_write__)
        # reader
        self._rbuf = bytearray()
        self._rtasks = deque()
        self._reader = loop.io(socket, EV_READ, self.__on_read__)
        self._reader.start()

    def __init__(self, *args, **kwargs):
        self.__setup__(*args, **kwargs)
        self._logger.debug(f"{self}: ready")

    def __del__(self):
        self.close()

    def __on_error__(self, message, level=ERROR, exc_info=True):
        try:
            suffix = " -> closing" if not self._closing else ""
            self._logger.log(level, f"{self}: {message}{suffix}", exc_info=exc_info)
        finally:
            self.close() # close on error

    def __run__(self, cb, *args):
        try:
            cb(*args)
        except Exception:
            self.__on_error__(f"error in {cb.__qualname__} callback")


    # close --------------------------------------------------------------------

    @property
    def closed(self):
        return self._socket.closed

    def __stop__(self):
        self._reader.stop()
        self._rtasks.clear()
        self._rbuf.clear()
        self._writer.stop()
        self._wtasks.clear()
        self._socket.close()

    def __cleanup__(self):
        self._reader = self._writer = None # break cycles

    def close(self, notify=True):
        if not self.closed and not self._closing:
            self._closing = True
            self._logger.debug(f"{self}: closing...")
            try:
                self.__stop__()
            finally:
                self.__cleanup__()
                if self._on_close:
                    cb, self._on_close = self._on_close, None
                    if notify:
                        self.__run__(cb, self)
                self._logger.debug(f"{self}: closed")
                self._closing = False


    # read ---------------------------------------------------------------------

    def __consume__(self, size, cb, args):
        if len(self._rbuf) >= size:
            buf, self._rbuf = self._rbuf[:size], self._rbuf[size:]
            self.__run__(cb, buf, *args)
            return True
        return False

    def __on_read__(self, *args): # watcher callback
        try:
            closed = self._socket.read(self._rbuf)
        except BlockingIOError:
           pass
        except Exception:
            self.__on_error__("error while reading data")
        else:
            while self._rtasks:
                task = self._rtasks.popleft()
                if not self.__consume__(*task):
                    self._rtasks.appendleft(task)
                    break
            if closed:
                # remote end closed the connection
                self.__on_error__("closed by peer", level=DEBUG, exc_info=False)

    def read(self, size, cb, *args):
        if size and (self._rtasks or not self.__consume__(size, cb, args)):
            if self.closed:
                raise ConnectionError(f"{self}: already closed.")
            self._rtasks.append((size, cb, args))


    # write --------------------------------------------------------------------

    def __on_write__(self, *args): # watcher callback
        buf, cb, args = task = self._wtasks.popleft()
        try:
            self._socket.write(buf)
        except BlockingIOError:
            self._wtasks.appendleft(task)
        except Exception:
            self.__on_error__("error while writing data")
        else:
            if buf:
                self._wtasks.appendleft(task)
            else:
                if not self._wtasks:
                    self._writer.stop()
                if cb:
                    self.__run__(cb, *args)

    def write(self, buf, cb=None, args=()):
        if buf:
            if self.closed:
                raise ConnectionError(f"{self}: already closed.")
            self._wtasks.append((buf, cb, args))
            if not self._writer.active:
                self._writer.start()


# ------------------------------------------------------------------------------
# Overwatch

class Overwatch(Connection):

    def __setup__(self, socket, loop, logger, on_close=None):
        self._loop = Loop(flags=EVFLAG_NOSIGMASK)
        super().__setup__(socket, self._loop, logger, on_close=on_close)
        # overwatch
        self._overwatch = loop.io(socket, EV_READ, self.__on_read__)
        self._overwatch.start()

    def __stop__(self):
        self._loop.stop(EVBREAK_ALL)
        self._overwatch.stop()
        super().__stop__()

    def __cleanup__(self):
        self._overwatch = None # break cycles
        super().__cleanup__()

    def __block__(self):
        self._overwatch.stop()
        self._loop.start()

    def __unblock__(self):
        self._loop.stop(EVBREAK_ALL)
        self._overwatch.start()

