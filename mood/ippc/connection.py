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


from logging import getLogger, ERROR, DEBUG
from collections import deque

from mood.event import EV_READ, EV_WRITE


# ------------------------------------------------------------------------------
# Connection

class Connection(object):

    def __setup__(self, socket, loop, **kwargs):
        self._socket_ = socket
        self.logger = kwargs.pop("logger", getLogger(__name__))
        self._on_close_ = kwargs.pop("on_close", None)
        self._closing_ = False
        # writer
        self._wtasks_ = deque()
        self._writer_ = loop.io(socket, EV_WRITE, self.__on_write__)
        # reader
        self._rbuf_ = bytearray()
        self._rtasks_ = deque()
        self._reader_ = loop.io(socket, EV_READ, self.__on_read__)
        self._reader_.start()

    def __init__(self, *args, **kwargs):
        self.__setup__(*args, **kwargs)
        self.logger.debug(f"{self}: ready")

    def __del__(self):
        self.close()

    def __on_error__(self, message, level=ERROR, exc_info=True):
        try:
            suffix = " -> closing" if not self._closing_ else ""
            self.logger.log(
                level, f"{self}: {message}{suffix}", exc_info=exc_info
            )
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
        return self._socket_.closed

    def __stop__(self):
        self._reader_.stop()
        self._rtasks_.clear()
        self._rbuf_.clear()
        self._writer_.stop()
        self._wtasks_.clear()
        self._socket_.close()

    def __cleanup__(self):
        self._reader_ = self._writer_ = None # break cycles

    def close(self, notify=True):
        if not self.closed and not self._closing_:
            self._closing_ = True
            self.logger.debug(f"{self}: closing...")
            try:
                self.__stop__()
            finally:
                self.__cleanup__()
                if self._on_close_:
                    cb, self._on_close_ = self._on_close_, None
                    if notify:
                        self.__run__(cb, self)
                self.logger.debug(f"{self}: closed")
                self._closing_ = False


    # read ---------------------------------------------------------------------

    def __consume__(self, size, cb, args):
        if len(self._rbuf_) >= size:
            buf, self._rbuf_ = self._rbuf_[:size], self._rbuf_[size:]
            self.__run__(cb, buf, *args)
            return True
        return False

    def __on_read__(self, *args): # watcher callback
        try:
            closed = self._socket_.read(self._rbuf_)
        except BlockingIOError:
           pass
        except Exception:
            self.__on_error__("error while reading data")
        else:
            while self._rtasks_:
                task = self._rtasks_.popleft()
                if not self.__consume__(*task):
                    self._rtasks_.appendleft(task)
                    break
            if closed:
                # remote end closed the connection
                self.__on_error__("closed by peer", level=DEBUG, exc_info=False)

    def read(self, size, cb, *args):
        if size and (self._rtasks_ or not self.__consume__(size, cb, args)):
            if self.closed:
                raise ConnectionError(f"{self}: already closed.")
            self._rtasks_.append((size, cb, args))


    # write --------------------------------------------------------------------

    def __on_write__(self, *args): # watcher callback
        buf, cb, args = task = self._wtasks_.popleft()
        try:
            self._socket_.write(buf)
        except BlockingIOError:
            self._wtasks_.appendleft(task)
        except Exception:
            self.__on_error__("error while writing data")
        else:
            if buf:
                self._wtasks_.appendleft(task)
            else:
                if not self._wtasks_:
                    self._writer_.stop()
                if cb:
                    self.__run__(cb, *args)

    def write(self, buf, cb=None, args=()):
        if buf:
            if self.closed:
                raise ConnectionError(f"{self}: already closed.")
            self._wtasks_.append((buf, cb, args))
            if not self._writer_.active:
                self._writer_.start()
