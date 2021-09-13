/*
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
*/


#define PY_SSIZE_T_CLEAN
#include "Python.h"


#include "helpers/helpers.h"


#include <stddef.h>

#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/un.h>


#define SOCK_TYPE (SOCK_STREAM | SOCK_CLOEXEC)
#define SOCK_FLAGS (SOCK_CLOEXEC | SOCK_NONBLOCK)


static int
getsocksize(int fd)
{
    int result;
    socklen_t resultlen = sizeof(result);

    if (getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &result, &resultlen)) {
        return -1;
    }
    return ((result / 2) & ~7);
}


/* --------------------------------------------------------------------------
   Abstract
   -------------------------------------------------------------------------- */

/* Abstract */
typedef struct {
    PyObject_HEAD
    PyObject *name;
    int fd;
    int size;
    int server;
} Abstract;


static inline Abstract *
__socket_alloc(PyTypeObject *type, int server)
{
    Abstract *self = NULL;

    if ((self = PyObject_GC_NEW(Abstract, type))) {
        self->name = NULL;
        self->fd = -1;
        self->size = -1;
        self->server = server;
        PyObject_GC_Track(self);
    }
    return self;
}


static inline int
__socket_init(Abstract *self, PyObject *args, PyObject *kwargs)
{
    PyObject *name = NULL;
    const char *_name_ = NULL;
    Py_ssize_t namelen = 0;
    struct sockaddr_un addr = { .sun_family = AF_UNIX, .sun_path = "" };
    socklen_t addrlen = offsetof(struct sockaddr_un, sun_path) + 1;
    int nbio = 1;

    if (
        !PyArg_ParseTuple(args, "U:__new__", &name) ||
        !(_name_ = PyUnicode_AsUTF8AndSize(name, &namelen))
    ) {
        return -1;
    }
    if (!namelen) {
        PyErr_SetString(PyExc_ValueError, "Invalid argument");
        return -1;
    }
    if ((size_t)namelen >= sizeof(addr.sun_path)) {
        PyErr_SetString(PyExc_ValueError, "Name too long");
        return -1;
    }
    if ((size_t)namelen != strlen(_name_)) {
        PyErr_SetString(PyExc_ValueError, "Embedded null character");
        return -1;
    }
    memcpy(addr.sun_path + 1, _name_, namelen); //abstract namespace
    addrlen += namelen;
    if (
        ((self->fd = socket(AF_UNIX, SOCK_TYPE, 0)) == -1) ||
        ((self->size = getsocksize(self->fd)) == -1) ||
        (
            (self->server) ?
            (bind(self->fd, &addr, addrlen) || listen(self->fd, SOMAXCONN)) :
            connect(self->fd, &addr, addrlen)
        ) ||
        ioctl(self->fd, FIONBIO, &nbio)
    ) {
        _PyErr_SetFromErrno();
        return -1;
    }
    _Py_SET_MEMBER(self->name, name);
    return 0;
}


static inline PyObject *
__socket_new(PyTypeObject *type, PyObject *args, PyObject *kwargs, int server)
{
    Abstract *self = NULL;

    if (
        (self = __socket_alloc(type, server)) &&
        __socket_init(self, args, kwargs)
    ) {
        Py_CLEAR(self);
    }
    return (PyObject *)self;
}


static inline int
__socket_close(Abstract *self)
{
    int res = 0;

    if (self->fd != -1) {
        if ((res = close(self->fd))) {
            _PyErr_SetFromErrno();
        }
        self->fd = -1;
    }
    return res;
}


/* Abstract_Type ------------------------------------------------------------ */

/* Abstract_Type.tp_traverse */
static int
Abstract_tp_traverse(Abstract *self, visitproc visit, void *arg)
{
    Py_VISIT(self->name);
    return 0;
}


/* Abstract_Type.tp_clear */
static int
Abstract_tp_clear(Abstract *self)
{
    Py_CLEAR(self->name);
    return 0;
}


/* Abstract_Type.tp_dealloc */
static void
Abstract_tp_dealloc(Abstract *self)
{
    if (PyObject_CallFinalizerFromDealloc((PyObject *)self)) {
        return;
    }
    PyObject_GC_UnTrack(self);
    Abstract_tp_clear(self);
    PyObject_GC_Del(self);
}


/* Abstract_Type.tp_repr */
static PyObject *
Abstract_tp_repr(Abstract *self)
{
    return PyUnicode_FromFormat(
        "<%s(name='%U', fd=%d)>", Py_TYPE(self)->tp_name, self->name, self->fd
    );
}


/* Abstract.close() */
PyDoc_STRVAR(Abstract_close_doc,
"close()");

static PyObject *
Abstract_close(Abstract *self)
{
    return (__socket_close(self)) ? NULL : __Py_INCREF(Py_None);
}


/* Abstract.fileno() */
PyDoc_STRVAR(Abstract_fileno_doc,
"fileno() -> int");

static PyObject *
Abstract_fileno(Abstract *self)
{
    return PyLong_FromLong(self->fd);
}


/* Abstract_Type.tp_methods */
static PyMethodDef Abstract_tp_methods[] = {
    {
        "close", (PyCFunction)Abstract_close,
        METH_NOARGS, Abstract_close_doc
    },
    {
        "fileno", (PyCFunction)Abstract_fileno,
        METH_NOARGS, Abstract_fileno_doc
    },
    {NULL}  /* Sentinel */
};


/* Abstract.closed */
static PyObject *
Abstract_closed_get(Abstract *self, void *closure)
{
    return PyBool_FromLong((self->fd == -1));
}


/* Abstract_Type.tp_getset */
static PyGetSetDef Abstract_tp_getset[] = {
    {
        "closed", (getter)Abstract_closed_get,
        _Py_READONLY_ATTRIBUTE, NULL, NULL
    },
    {NULL}  /* Sentinel */
};


/* Abstract_Type.tp_finalize */
static void
Abstract_tp_finalize(Abstract *self)
{
    PyObject *exc_type, *exc_value, *exc_traceback;

    PyErr_Fetch(&exc_type, &exc_value, &exc_traceback);
    if (__socket_close(self)) {
        PyErr_WriteUnraisable((PyObject *)self);
    }
    PyErr_Restore(exc_type, exc_value, exc_traceback);
}


static PyTypeObject Abstract_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "mood.ippc.sockets.Abstract",
    .tp_basicsize = sizeof(Abstract),
    .tp_dealloc = (destructor)Abstract_tp_dealloc,
    .tp_repr = (reprfunc)Abstract_tp_repr,
    .tp_flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_HAVE_FINALIZE),
    .tp_traverse = (traverseproc)Abstract_tp_traverse,
    .tp_clear = (inquiry)Abstract_tp_clear,
    .tp_methods = Abstract_tp_methods,
    .tp_getset = Abstract_tp_getset,
    .tp_finalize = (destructor)Abstract_tp_finalize,
};


/* --------------------------------------------------------------------------
   Socket
   -------------------------------------------------------------------------- */

static inline Py_ssize_t
__buf_terminate(PyByteArrayObject *buf, Py_ssize_t size)
{
    Py_SIZE(buf) = size;
    buf->ob_start[size] = '\0';
    return size;
}


static inline int
__buf_realloc(PyByteArrayObject *buf, Py_ssize_t nalloc)
{
    Py_ssize_t alloc = 0;
    void *bytes = NULL;

    if (buf->ob_alloc < nalloc) {
        alloc = Py_MAX(nalloc, (buf->ob_alloc << 1));
        if (!(bytes = PyObject_Realloc(buf->ob_bytes, alloc))) {
            return -1;
        }
        buf->ob_start = buf->ob_bytes = bytes;
        buf->ob_alloc = alloc;
    }
    return 0;
}


static inline int
__buf_resize(PyByteArrayObject *buf, size_t size)
{
    if ((size >= PY_SSIZE_T_MAX) || __buf_realloc(buf, (size + 1))) {
        PyErr_NoMemory();
        return -1;
    }
    return 0;
}


/* Socket_Type -------------------------------------------------------------- */

/* Socket.write(buf) */
PyDoc_STRVAR(Socket_write_doc,
"write(buf)");

static PyObject *
Socket_write(Abstract *self, PyObject *args)
{
    PyByteArrayObject *buf = NULL;
    Py_ssize_t len = 0, size = -1;

    if (!PyArg_ParseTuple(args, "Y:write", &buf)) {
        return NULL;
    }
    len = Py_SIZE(buf);
    while (len > 0) {
        size = write(self->fd, buf->ob_start, Py_MIN(len, self->size));
        if (size == -1) {
            return _PyErr_SetFromErrno();
        }
        // XXX: very bad shortcut ¯\_(ツ)_/¯
        buf->ob_start += size;
        len = __buf_terminate(buf, (len - size));
    }
    Py_RETURN_NONE;
}


/* Socket.read(buf) */
PyDoc_STRVAR(Socket_read_doc,
"read(buf) -> bool");

static PyObject *
Socket_read(Abstract *self, PyObject *args)
{
    PyByteArrayObject *buf = NULL;
    Py_ssize_t len = 0, size = -1;
    int nread = 0;

    if (!PyArg_ParseTuple(args, "Y:read", &buf)) {
        return NULL;
    }
    len = Py_SIZE(buf);
    if (ioctl(self->fd, FIONREAD, &nread)) {
        return _PyErr_SetFromErrno();
    }
    if (nread && __buf_resize(buf, (len + nread))) {
        return NULL;
    }
    do {
        size = read(self->fd, (buf->ob_start + len), nread);
        if (size == -1) {
            return _PyErr_SetFromErrno();
        }
        if (size) {
            nread -= size;
            len = __buf_terminate(buf, (len + size));
        }
    } while (nread > 0);
    return PyBool_FromLong((size == 0));
}


/* Socket_Type.tp_methods */
static PyMethodDef Socket_tp_methods[] = {
    {
        "write", (PyCFunction)Socket_write,
        METH_VARARGS, Socket_write_doc
    },
    {
        "read", (PyCFunction)Socket_read,
        METH_VARARGS, Socket_read_doc
    },
    {NULL}  /* Sentinel */
};


static PyTypeObject Socket_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "mood.ippc.sockets.Socket",
    .tp_basicsize = sizeof(Abstract),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = Socket_tp_methods,
};


/* --------------------------------------------------------------------------
   Server
   -------------------------------------------------------------------------- */

/* Server.accept() */
PyDoc_STRVAR(Server_accept_doc,
"accept() -> Socket");

static PyObject *
Server_accept(Abstract *self)
{
    Abstract *result = NULL;

    if ((result = __socket_alloc(&Socket_Type, 0))) {
        if (
            ((result->fd = accept4(self->fd, NULL, NULL, SOCK_FLAGS)) == -1) ||
            ((result->size = getsocksize(result->fd)) == -1)
        ) {
            Py_CLEAR(result);
            return _PyErr_SetFromErrno();
        }
        _Py_SET_MEMBER(result->name, self->name);
    }
    return (PyObject *)result;
}


/* Server_Type.tp_methods */
static PyMethodDef Server_tp_methods[] = {
    {
        "accept", (PyCFunction)Server_accept,
        METH_NOARGS, Server_accept_doc
    },
    {NULL}  /* Sentinel */
};


/* Server_Type.tp_new */
static PyObject *
Server_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    return __socket_new(type, args, kwargs, 1);
}


static PyTypeObject Server_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "mood.ippc.sockets.ServerSocket",
    .tp_basicsize = sizeof(Abstract),
    .tp_flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE),
    .tp_methods = Server_tp_methods,
    .tp_new = Server_tp_new,
};


/* --------------------------------------------------------------------------
   Client
   -------------------------------------------------------------------------- */

/* Client_Type.tp_new */
static PyObject *
Client_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    return __socket_new(type, args, kwargs, 0);
}


static PyTypeObject Client_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "mood.ippc.sockets.ClientSocket",
    .tp_basicsize = sizeof(Abstract),
    .tp_flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE),
    .tp_new = Client_tp_new,
};


/* --------------------------------------------------------------------------
   module
   -------------------------------------------------------------------------- */

/* sockets_def */
static PyModuleDef sockets_def = {
    PyModuleDef_HEAD_INIT,
    .m_name = "ippc.sockets",
    .m_doc = "ippc.sockets module",
    .m_size = 0,
};


static inline int
_module_init(PyObject *module)
{
    if (
        PyModule_AddStringConstant(module, "__version__", PKG_VERSION) ||
        PyType_Ready(&Abstract_Type) ||
        _PyType_ReadyWithBase(&Socket_Type, &Abstract_Type) ||
        _PyModule_AddTypeWithBase(
            module, "ServerSocket", &Server_Type, &Abstract_Type
        ) ||
        _PyModule_AddTypeWithBase(
            module, "ClientSocket", &Client_Type, &Socket_Type
        )
    ) {
        return -1;
    }
    return 0;
}


/* module initialization */
PyMODINIT_FUNC
PyInit_sockets(void)
{
    PyObject *module = NULL;

    if ((module = PyState_FindModule(&sockets_def))) {
        Py_INCREF(module);
    }
    else if ((module = PyModule_Create(&sockets_def)) && _module_init(module)) {
        Py_CLEAR(module);
    }
    return module;
}

