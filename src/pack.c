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


/* we need a 64bit type */
#if !defined(HAVE_LONG_LONG)
#error "mood.ippc.pack needs a long long integer type"
#endif /* HAVE_LONG_LONG */


/* for float conversion */
typedef union {
    double f;
    uint64_t i;
} float64_t;


/* module state */
typedef struct {
    PyObject *registry;
} module_state;


/* for use with Py_EnterRecursiveCall */
#define _While_(a, n) " while " #a " a " n
#define _Packing_(n) _While_(packing, n)
#define _Unpacking_(n) _While_(unpacking, n)


#define _PyBytes_FromPyByteArray(o) \
    PyBytes_FromStringAndSize(PyByteArray_AS_STRING(o), PyByteArray_GET_SIZE(o))


_Py_IDENTIFIER(__reduce__);


/* fwd */
static int __pack_object(PyObject *, PyObject *);
static PyObject *__unpack_msg(Py_buffer *, Py_ssize_t *);

static module_state *_module_get_state(void);


/* --------------------------------------------------------------------------
   definitions
   -------------------------------------------------------------------------- */

#define INT4_MAX (1LL << 31)
#define INT4_MIN -INT4_MAX

#define INT2_MAX (1LL << 15)
#define INT2_MIN -INT2_MAX

#define INT1_MAX (1LL << 7)
#define INT1_MIN -INT1_MAX


/* types */
enum {
    TYPE_INVALID   = 0x00, // invalid type

    TYPE_INT1      = 0x01,
    TYPE_INT2      = 0x02,
    TYPE_INT4      = 0x04,
    TYPE_INT8      = 0x08,

    TYPE_UINT      = 0x11,
    TYPE_FLOAT     = 0x12,
    TYPE_COMPLEX   = 0x13,

    TYPE_NONE      = 0x21,
    TYPE_TRUE      = 0x22,
    TYPE_FALSE     = 0x23,

    TYPE_STR       = 0x30,
    TYPE_BYTES     = 0x40,
    TYPE_BYTEARRAY = 0x50,

    TYPE_TUPLE     = 0x60,
    TYPE_LIST      = 0x70,

    TYPE_DICT      = 0x80,

    TYPE_SET       = 0x90,
    TYPE_FROZENSET = 0xa0,

    TYPE_CLASS     = 0xd0,
    TYPE_SINGLETON = 0xe0,
    TYPE_INSTANCE  = 0xf0,
};


/* --------------------------------------------------------------------------
   pack
   -------------------------------------------------------------------------- */

static inline PyByteArrayObject *
__msg_new__(Py_ssize_t alloc)
{
    PyByteArrayObject *self = NULL;

    if ((self = PyObject_New(PyByteArrayObject, &PyByteArray_Type))) {
        if ((self->ob_bytes = PyObject_Malloc(alloc))) {
            self->ob_start = self->ob_bytes;
            self->ob_alloc = alloc;
            self->ob_exports = 0;
            Py_SIZE(self) = 0;
            self->ob_bytes[0] = '\0';
        }
        else {
            Py_CLEAR(self);
            PyErr_NoMemory();
        }
    }
    return self;
}


static inline int
__msg_resize__(PyByteArrayObject *self, Py_ssize_t nalloc)
{
    Py_ssize_t alloc = 0;
    void *bytes = NULL;

    if (self->ob_alloc < nalloc) {
        alloc = Py_MAX(nalloc, (self->ob_alloc << 1));
        if (!(bytes = PyObject_Realloc(self->ob_bytes, alloc))) {
            return -1;
        }
        self->ob_start = self->ob_bytes = bytes;
        self->ob_alloc = alloc;
    }
    return 0;
}


#define __PACK_BEGIN__ \
    size_t start = Py_SIZE(self), nsize = start + size; \
    if ((nsize >= PY_SSIZE_T_MAX) || __msg_resize__(self, (nsize + 1))) { \
        PyErr_NoMemory(); \
        return -1; \
    }


#define __PACK_END__ \
    Py_SIZE(self) = nsize; \
    self->ob_bytes[nsize] = '\0'; \
    return 0;


static inline int
__pack_type__(PyByteArrayObject *self, uint8_t type)
{
    static const size_t size = 1;

    __PACK_BEGIN__

    self->ob_bytes[start] = type;

    __PACK_END__
}


static inline int
__pack_buffer__(PyByteArrayObject *self, uint8_t type,
                const void *_buffer, size_t _size)
{
    size_t size = 1 + _size;

    __PACK_BEGIN__

    self->ob_bytes[start++] = type;
    memcpy((self->ob_bytes + start), _buffer, _size);

    __PACK_END__
}


static inline int
__pack_buffers__(PyByteArrayObject *self, uint8_t type,
                 const void *_buffer1, size_t _size1,
                 const void *_buffer2, size_t _size2)
{
    size_t size = 1 + _size1 + _size2;

    __PACK_BEGIN__

    self->ob_bytes[start++] = type;
    memcpy((self->ob_bytes + start), _buffer1, _size1);
    start += _size1;
    memcpy((self->ob_bytes + start), _buffer2, _size2);

    __PACK_END__
}


/* -------------------------------------------------------------------------- */

static PyObject *
__msg_new(Py_ssize_t alloc)
{
    return _PyObject_CAST(__msg_new__(((alloc + 7) & ~7)));
}

#define __new_msg() __msg_new(32)


static int
__pack_type(PyObject *msg, uint8_t type)
{
    return __pack_type__((PyByteArrayObject *)msg, type);
}


static int
__pack_buffer(PyObject *msg, uint8_t type,
              const void *_buffer, size_t _size)
{
    return __pack_buffer__((PyByteArrayObject *)msg, type,
                           _buffer, _size);
}


static int
__pack_buffers(PyObject *msg, uint8_t type,
               const void *_buffer1, size_t _size1,
               const void *_buffer2, size_t _size2)
{
    return __pack_buffers__((PyByteArrayObject *)msg, type,
                            _buffer1, _size1,
                            _buffer2, _size2);
}


/* -------------------------------------------------------------------------- */

static inline uint8_t
__size__(Py_ssize_t l)
{
    return (l < INT2_MAX) ? (l < INT1_MAX) ? 1 : 2 : (l < INT4_MAX) ? 4 : 8;
}


static inline int
__pack_len(PyObject *msg, uint8_t type, Py_ssize_t len)
{
    uint8_t size =  __size__(len);

    return __pack_buffer(msg, (type | size), &len, size);
}


static inline int
__pack_data(PyObject *msg, uint8_t type, const void *data, Py_ssize_t len)
{
    uint8_t size =  __size__(len);

    return __pack_buffers(msg, (type | size), &len, size, data, len);
}


/* TYPE_INT / TYPE_UINT ----------------------------------------------------- */

static inline int
__pack_int__(PyObject *msg, int64_t value)
{
    int res = -1;

    if (value < 0) {
        if (value < INT2_MIN) {
            if (value < INT4_MIN) {
                res = __pack_buffer(msg, TYPE_INT8, &value, 8);
            }
            else {
                res = __pack_buffer(msg, TYPE_INT4, &value, 4);
            }
        }
        else {
            if (value < INT1_MIN) {
                res = __pack_buffer(msg, TYPE_INT2, &value, 2);
            }
            else {
                res = __pack_buffer(msg, TYPE_INT1, &value, 1);
            }
        }
    }
    else {
        if (value < INT2_MAX) {
            if (value < INT1_MAX) {
                res = __pack_buffer(msg, TYPE_INT1, &value, 1);
            }
            else {
                res = __pack_buffer(msg, TYPE_INT2, &value, 2);
            }
        }
        else {
            if (value < INT4_MAX) {
                res = __pack_buffer(msg, TYPE_INT4, &value, 4);
            }
            else {
                res = __pack_buffer(msg, TYPE_INT8, &value, 8);
            }
        }
    }
    return res;
}

static inline int
__pack_int(PyObject *msg, int64_t value)
{
    if ((value == -1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_int__(msg, value);
}


#define __pack_uint__(m, v) __pack_buffer(m, TYPE_UINT, &v, 8)

static inline int
__pack_uint(PyObject *msg, uint64_t value)
{
    if ((value == (uint64_t)-1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_uint__(msg, value);
}


static inline int
__pack_long(PyObject *msg, PyObject *obj)
{
    int overflow = 0;
    int64_t value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if (overflow) {
        if (overflow < 0) {
            PyErr_SetString(PyExc_OverflowError, "int too big to convert");
            return -1;
        }
        return __pack_uint(msg, PyLong_AsUnsignedLongLong(obj));
    }
    return __pack_int(msg, value);
}


/* TYPE_FLOAT --------------------------------------------------------------- */

#define __pack_float__(m, v) __pack_buffer(m, TYPE_FLOAT, &v, 8)

static inline int
__pack_float(PyObject *msg, PyObject *obj)
{
    float64_t fvalue = { .f = PyFloat_AS_DOUBLE(obj) };

    return __pack_float__(msg, fvalue.i);
}


/* TYPE_COMPLEX ------------------------------------------------------------- */

#define __pack_complex__(m, r, i) __pack_buffers(m, TYPE_COMPLEX, &r, 8, &i, 8)

static inline int
__pack_complex(PyObject *msg, PyObject *obj)
{
    Py_complex complex = ((PyComplexObject *)obj)->cval;
    float64_t freal = { .f = complex.real}, fimag = { .f = complex.imag};

    return __pack_complex__(msg, freal.i, fimag.i);
}


/* TYPE_NONE / TYPE_TRUE/ TYPE_FALSE ---------------------------------------- */

#define __pack_none(m) __pack_type(m, TYPE_NONE)

#define __pack_true(m) __pack_type(m, TYPE_TRUE)

#define __pack_false(m) __pack_type(m, TYPE_FALSE)


/* TYPE_STR ----------------------------------------------------------------- */

static inline int
__pack_unicode(PyObject *msg, uint8_t type, PyObject *obj)
{
    const char *bytes = NULL;
    Py_ssize_t len = 0;

    if (!(bytes = PyUnicode_AsUTF8AndSize(obj, &len))) {
        return -1;
    }
    return __pack_data(msg, type, bytes, len);
}

#define __pack_str(m, o) __pack_unicode(m, TYPE_STR, o)


/* TYPE_BYTES / TYPE_BYTEARRAY ---------------------------------------------- */

#define __pack_bin(T, m, t, o) \
    __pack_data(m, t, T##_AS_STRING(o), T##_GET_SIZE(o))


#define __pack_bytes(m, o) __pack_bin(PyBytes, m, TYPE_BYTES, o)

#define __pack_bytearray(m, o) __pack_bin(PyByteArray, m, TYPE_BYTEARRAY, o)


/* TYPE_TUPLE / TYPE_LIST --------------------------------------------------- */

static inline int
__pack_sequence__(PyObject *msg, uint8_t type, PyObject **items, Py_ssize_t len,
                  const char *where)
{
    Py_ssize_t i;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_len(msg, type, len)) {
            for (res = 0, i = 0; i < len; ++i) {
                if ((res = __pack_object(msg, items[i]))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_sequence(T, m, t, o, n) \
    __pack_sequence__(m, t, _##T##_ITEMS(o), T##_GET_SIZE(o), _Packing_(n))


#define __pack_tuple(m, o) __pack_sequence(PyTuple, m, TYPE_TUPLE, o, "tuple")

#define __pack_list(m, o) __pack_sequence(PyList, m, TYPE_LIST, o, "list")


/* TYPE_DICT ---------------------------------------------------------------- */

static inline int
__pack_dict(PyObject *msg, PyObject *obj)
{
    Py_ssize_t pos = 0;
    PyObject *key = NULL, *val = NULL;
    int res = -1;

    if (!Py_EnterRecursiveCall( _Packing_("dict"))) {
        if (!__pack_len(msg, TYPE_DICT, PyDict_GET_SIZE(obj))) {
            while ((res = PyDict_Next(obj, &pos, &key, &val))) {
                if ((res = __pack_object(msg, key)) ||
                    (res = __pack_object(msg, val))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


/* TYPE_SET / TYPE_FROZENSET ------------------------------------------------ */

static inline int
__pack_anyset__(PyObject *msg, uint8_t type, PyObject *obj, const char *where)
{
    Py_ssize_t pos = 0;
    PyObject *item = NULL;
    Py_hash_t hash;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_len(msg, type, PySet_GET_SIZE(obj))) {
            while ((res = _PySet_NextEntry(obj, &pos, &item, &hash))) {
                if ((res = __pack_object(msg, item))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_anyset(m, t, o, n) \
    __pack_anyset__(m, t, o, _Packing_(n))


#define __pack_set(m, o) __pack_anyset(m, TYPE_SET, o, "set")

#define __pack_frozenset(m, o) __pack_anyset(m, TYPE_FROZENSET, o, "frozenset")


/* TYPE_CLASS --------------------------------------------------------------- */

static inline int
__pack_class_id(PyObject *msg, PyObject *obj)
{
    _Py_IDENTIFIER(__module__);
    _Py_IDENTIFIER(__qualname__);
    PyObject *module = NULL, *qualname = NULL;
    int res = -1;

    if ((module = _PyObject_GetAttrId(obj, &PyId___module__)) &&
        (qualname = _PyObject_GetAttrId(obj, &PyId___qualname__))) {
        if (PyUnicode_CheckExact(module) && PyUnicode_CheckExact(qualname)) {
            res = __pack_str(msg, module) ? -1 : __pack_str(msg, qualname);
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "expected strings, got: __module__: %.200s, __qualname__: %.200s",
                         Py_TYPE(module)->tp_name, Py_TYPE(qualname)->tp_name);
        }
    }
    Py_XDECREF(qualname);
    Py_XDECREF(module);
    return res;
}

#define __pack_class__(m, d, o) \
    ((__pack_class_id(d, o)) ? -1 : __pack_bin(PyByteArray, m, TYPE_CLASS, d))

static inline int
__pack_class(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_class__(msg, data, obj);
        Py_DECREF(data);
    }
    return res;
}


/* TYPE_SINGLETON ----------------------------------------------------------- */

static inline int
__pack_singleton_id(PyObject *msg, PyObject *obj)
{
    PyObject *reduce = NULL;
    int res = -1;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if (PyUnicode_CheckExact(reduce)) {
            res = __pack_str(msg, reduce);
        }
        else {
            PyErr_SetString(PyExc_TypeError, "__reduce__() must return a str");
        }
        Py_DECREF(reduce);
    }
    return res;
}


/* TYPE_INSTANCE ------------------------------------------------------------ */

#define __pack_reduce_singleton__(m, o) \
    (__pack_str(m, o) ? TYPE_INVALID : TYPE_SINGLETON)

#define __pack_reduce_instance__(m, o) \
    (__pack_tuple(m, o) ? TYPE_INVALID : TYPE_INSTANCE)

#define __pack_instance__(m, t, o) __pack_bin(PyByteArray, m, t, o)

static inline int
__pack_instance(PyObject *msg, PyObject *obj, const char *name)
{
    PyObject *reduce = NULL, *data = NULL;
    uint8_t type = TYPE_INVALID; // 0
    int res = -1;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if ((data = __new_msg())) {
            if (PyUnicode_CheckExact(reduce)) {
                type = __pack_reduce_singleton__(data, reduce);
            }
            else if (PyTuple_CheckExact(reduce)) {
                type = __pack_reduce_instance__(data, reduce);
            }
            else {
                PyErr_SetString(PyExc_TypeError,
                                "__reduce__() must return a str or a tuple");
            }
            if (type) {
                res = __pack_instance__(msg, type, data);
            }
            Py_DECREF(data);
        }
        Py_DECREF(reduce);
    }
    else if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        PyErr_Format(PyExc_TypeError, "cannot pack '%.200s' objects", name);
    }
    return res;
}


/* -------------------------------------------------------------------------- */

#define __pack_register(m, o) \
    (PyType_Check(o) ? __pack_class_id(m, o) : __pack_singleton_id(m, o))

static int
__register_object(PyObject *registry, PyObject *obj)
{
    PyObject *msg = NULL, *key = NULL;
    int res = -1;

    if ((msg = __new_msg())) {
        if (!__pack_register(msg, obj) && (key = _PyBytes_FromPyByteArray(msg))) {
            res = PyDict_SetItem(registry, key, obj);
            Py_DECREF(key);
        }
        Py_DECREF(msg);
    }
    return res;
}


static inline int
__pack_object__(PyObject *msg, PyObject *obj, PyTypeObject *type)
{
    int res = -1;

    if (type == &PyLong_Type) {
        res = __pack_long(msg, obj);
    }
    else if (type == &PyFloat_Type) {
        res = __pack_float(msg, obj);
    }
    else if (type == &PyComplex_Type) {
        res = __pack_complex(msg, obj);
    }
    else if (type == &PyUnicode_Type) {
        res = __pack_str(msg, obj);
    }
    else if (type == &PyBytes_Type) {
        res = __pack_bytes(msg, obj);
    }
    else if (type == &PyByteArray_Type) {
        res = __pack_bytearray(msg, obj);
    }
    else if (type == &PyTuple_Type) {
        res = __pack_tuple(msg, obj);
    }
    else if (type == &PyList_Type) {
        res = __pack_list(msg, obj);
    }
    else if (type == &PyDict_Type) {
        res = __pack_dict(msg, obj);
    }
    else if (type == &PySet_Type) {
        res = __pack_set(msg, obj);
    }
    else if (type == &PyFrozenSet_Type) {
        res = __pack_frozenset(msg, obj);
    }
    else if (type == &PyType_Type) {
        res = __pack_class(msg, obj);
    }
    else {
        res = __pack_instance(msg, obj, type->tp_name);
    }
    return res;
}


static int
__pack_object(PyObject *msg, PyObject *obj)
{
    int res = -1;

    if (obj == Py_None) {
        res = __pack_none(msg);
    }
    else if (obj == Py_True) {
        res = __pack_true(msg);
    }
    else if (obj == Py_False) {
        res = __pack_false(msg);
    }
    else {
        res = __pack_object__(msg, obj, Py_TYPE(obj));
    }
    return res;
}


static inline PyObject *
__pack_encode__(PyObject *msg)
{
    PyObject *result = NULL;
    Py_ssize_t len = PyByteArray_GET_SIZE(msg);
    uint8_t size = __size__(len);

    if ((result = __msg_new(2 + size + len)) &&
        __pack_buffers(result, size, &len, size, PyByteArray_AS_STRING(msg), len)) {
        Py_CLEAR(result);
    }
    return result;
}


static PyObject *
__pack_encode(PyObject *msg, PyObject *obj)
{
    return __pack_object(msg, obj) ? NULL : __pack_encode__(msg);
}


/* --------------------------------------------------------------------------
   unpack
   -------------------------------------------------------------------------- */

static inline const char *
__unpack_buffer(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    Py_ssize_t poff = *off, noff = poff + size;

    if (noff > msg->len) {
        PyErr_SetString(PyExc_EOFError, "Ran out of input");
        return NULL;
    }
    *off = noff;
    return ((msg->buf) + poff);
}


static inline uint8_t
__unpack_type(Py_buffer *msg, Py_ssize_t *off)
{
    const char *buffer = NULL;

    if ((buffer = __unpack_buffer(msg, off, 1))) {
        return (*((uint8_t *)buffer));
    }
    return TYPE_INVALID;
}


static inline PyObject *
__unpack_registered(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    const char *buffer = NULL;
    module_state *state = NULL;
    PyObject *result = NULL, *key = NULL;

    if ((buffer = __unpack_buffer(msg, off, size)) &&
        (state = _module_get_state()) &&
        (key = PyBytes_FromStringAndSize(buffer, size))) {
        if ((result = PyDict_GetItem(state->registry, key))) { // borrowed
            Py_INCREF(result);
        }
        Py_DECREF(key);
    }
    return result;
}


/* -------------------------------------------------------------------------- */

#define __unpack_int1__(b) (*((int8_t *)b))
#define __unpack_int2__(b) (*((int16_t *)b))
#define __unpack_int4__(b) (*((int32_t *)b))
#define __unpack_int8__(b) (*((int64_t *)b))


#define __unpack_uint8__(b) (*((uint64_t *)b))


static inline double
__unpack_float8__(const char *buffer)
{
    float64_t value = { .i = __unpack_uint8__(buffer) };

    return value.f;
}


/* -------------------------------------------------------------------------- */

#define __unpack_int__(b, s) \
    PyLong_FromLongLong(__unpack_int##s##__(b))


#define __unpack_uint__(b, s) \
    PyLong_FromUnsignedLongLong(__unpack_uint##s##__(b))


#define __unpack_float__(b, s) \
    PyFloat_FromDouble(__unpack_float##s##__(b))


#define __unpack_complex__(b, s) \
    PyComplex_FromDoubles(__unpack_float8__(b), __unpack_float8__((b + 8)))


#define __unpack_str__(b, s) \
    PyUnicode_FromStringAndSize(b, s)


#define __unpack_bytes__(b, s) \
    PyBytes_FromStringAndSize(b, s)


#define __unpack_bytearray__(b, s) \
    PyByteArray_FromStringAndSize(b, s)


static inline int
__unpack_sequence__(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size, PyObject **items)
{
    PyObject *item = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = ((item = __unpack_msg(msg, off)) ? 0 : -1))) {
            break;
        }
        items[i] = item; // steals ref
    }
    return res;
}


static inline int
__unpack_dict__(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size, PyObject *items)
{
    PyObject *key = NULL, *val = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = (((key = __unpack_msg(msg, off)) &&
                     (val = __unpack_msg(msg, off))) ?
                    PyDict_SetItem(items, key, val) : -1))) {
            Py_XDECREF(key);
            Py_XDECREF(val);
            break;
        }
        Py_DECREF(key);
        Py_DECREF(val);
    }
    return res;
}


static inline int
__unpack_anyset__(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size, PyObject *items)
{
    PyObject *item = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = ((item = __unpack_msg(msg, off)) ? PySet_Add(items, item) : -1))) {
            Py_XDECREF(item);
            break;
        }
        Py_DECREF(item);
    }
    return res;
}


/* -------------------------------------------------------------------------- */

#define __unpack_object(t, m, o, s) \
    ((buffer = __unpack_buffer(m, o, s)) ? __unpack_##t##__(buffer, s) : NULL)


#define __unpack_int(m, o, s) __unpack_object(int, m, o, s)


#define __unpack_uint(m, o, s) __unpack_object(uint, m, o, s)


#define __unpack_float(m, o, s) __unpack_object(float, m, o, s)


#define __unpack_complex(m, o, s) __unpack_object(complex, m, o, s)


#define __unpack_str(m, o, s) __unpack_object(str, m, o, s)


#define __unpack_bytes(m, o, s) __unpack_object(bytes, m, o, s)


#define __unpack_bytearray(m, o, s) __unpack_object(bytearray, m, o, s)


static PyObject *
__unpack_tuple(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("tuple"))) {
        if ((result = PyTuple_New(size)) &&
            __unpack_sequence__(msg, off, size, _PyTuple_ITEMS(result))) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}


static PyObject *
__unpack_list(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("list"))) {
        if ((result = PyList_New(size)) &&
            __unpack_sequence__(msg, off, size, _PyList_ITEMS(result))) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}


static PyObject *
__unpack_dict(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("dict"))) {
        if ((result = PyDict_New()) &&
            __unpack_dict__(msg, off, size, result)) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}


static PyObject *
__unpack_set(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("set"))) {
        if ((result = PySet_New(NULL)) &&
            __unpack_anyset__(msg, off, size, result)) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}


static PyObject *
__unpack_frozenset(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("frozenset"))) {
        if ((result = PyFrozenSet_New(NULL)) &&
            __unpack_anyset__(msg, off, size, result)) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}


/* -------------------------------------------------------------------------- */

static inline void
__unpack_class_error(Py_buffer *msg, Py_ssize_t *off)
{
    _Py_IDENTIFIER(builtins);
    PyObject *module = NULL, *qualname = NULL;

    if ((module = __unpack_msg(msg, off)) &&
        (qualname = __unpack_msg(msg, off))) {
        if (!_PyUnicode_EqualToASCIIId(module, &PyId_builtins)) {
            PyErr_Format(PyExc_TypeError,
                         "cannot unpack <class '%U.%U'>", module, qualname);
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "cannot unpack <class '%U'>", qualname);
        }
    }
    Py_XDECREF(qualname);
    Py_XDECREF(module);
}

static PyObject *
__unpack_class(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, off, size))) {
        __unpack_class_error(msg, &poff);
    }
    return result;
}


/* -------------------------------------------------------------------------- */

static inline void
__unpack_singleton_error(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *name = NULL;

    if ((name = __unpack_msg(msg, off))) {
        PyErr_Format(PyExc_TypeError,
                     "cannot unpack '%U'", name);
        Py_DECREF(name);
    }
}

static PyObject *
__unpack_singleton(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, off, size))) {
        __unpack_singleton_error(msg, &poff);
    }
    return result;
}


/* -------------------------------------------------------------------------- */

// object.__setstate__()
static inline int
__PyObject_UpdateDict(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(__dict__);
    Py_ssize_t pos = 0;
    PyObject *dict = NULL, *key = NULL, *value = NULL;
    int res = -1;

    if ((dict = _PyObject_GetAttrId(self, &PyId___dict__))) {
        while ((res = PyDict_Next(arg, &pos, &key, &value))) {
            /* normally the keys for instance attributes are interned.
               we should do that here. */
            Py_INCREF(key);
            if (!PyUnicode_Check(key)) {
                PyErr_Format(PyExc_TypeError,
                             "expected state key to be unicode, not '%.200s'",
                             Py_TYPE(key)->tp_name);
                Py_DECREF(key);
                break;
            }
            PyUnicode_InternInPlace(&key);
            /* __dict__ can be a dictionary or other mapping object
               https://docs.python.org/3.8/library/stdtypes.html#object.__dict__ */
            if ((res = PyObject_SetItem(dict, key, value))) {
                Py_DECREF(key);
                break;
            }
            Py_DECREF(key);
        }
        Py_DECREF(dict);
    }
    return res;
}

static int
__PyObject_SetState(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(__setstate__);
    PyObject *result = NULL;

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId___setstate__,
                                                 arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError) &&
            PyDict_Check(arg)) {
            PyErr_Clear();
            return __PyObject_UpdateDict(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
}


// object.extend()
static inline int
__PyObject_InPlaceConcatOrAdd(PyObject *self, PyObject *arg)
{
    PyTypeObject *type = Py_TYPE(self);
    PySequenceMethods *seq_methods = NULL;
    PyNumberMethods *num_methods = NULL;
    PyObject *result = NULL;
    int res = -1;

    if ((seq_methods = type->tp_as_sequence) &&
        seq_methods->sq_inplace_concat) {
        if ((result = seq_methods->sq_inplace_concat(self, arg))) {
            res = 0;
            Py_DECREF(result);
        }
    }
    else if ((num_methods = type->tp_as_number) &&
             num_methods->nb_inplace_add) {
        if ((result = num_methods->nb_inplace_add(self, arg))) {
            if (result != Py_NotImplemented) {
                res = 0;
            }
            Py_DECREF(result);
        }
    }
    if (res && !PyErr_Occurred()) {
        PyErr_Format(PyExc_TypeError,
                     "cannot extend '%.200s' objects", type->tp_name);
    }
    return res;
}

static int
__PyObject_Extend(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(extend);
    PyObject *result = NULL;

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId_extend,
                                                 arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return __PyObject_InPlaceConcatOrAdd(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
}


// object.update()
static PyObject *
__PySequence_Fast(PyObject *obj, Py_ssize_t len, const char *message)
{
    PyObject *result = NULL;

    if ((result = PySequence_Fast(obj, message)) &&
        (PySequence_Fast_GET_SIZE(result) != len)) {
        PyErr_Format(PyExc_ValueError,
                     "expected a sequence of len %zd", len);
        Py_CLEAR(result);
    }
    return result;
}

static inline int
__PyObject_MergeFromIter(PyObject *self, PyObject *iter)
{
    PyObject *item = NULL, *fast = NULL;

    while ((item = PyIter_Next(iter))) {
        if (!(fast = __PySequence_Fast(item, 2, "not a sequence")) ||
            PyObject_SetItem(self,
                             PySequence_Fast_GET_ITEM(fast, 0),
                             PySequence_Fast_GET_ITEM(fast, 1))) {
            Py_XDECREF(fast);
            Py_DECREF(item);
            break;
        }
        Py_DECREF(fast);
        Py_DECREF(item);
    }
    return PyErr_Occurred() ? -1 : 0;
}

static inline int
__PyObject_Merge(PyObject *self, PyObject *arg)
{
    PyObject *items = NULL, *iter = NULL;
    int res = -1;

    if (PyIter_Check(arg)) {
        Py_INCREF(arg);
        iter = arg;
    }
    else if ((items = PyMapping_Items(arg))) {
        iter = PyObject_GetIter(items);
        Py_DECREF(items);
    }
    else {
        PyErr_Clear();
        iter = PyObject_GetIter(arg);
    }
    if (iter) {
        res = __PyObject_MergeFromIter(self, iter);
        Py_DECREF(iter);
    }
    return res;
}

static int
__PyObject_Update(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(update);
    PyObject *result = NULL;

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId_update,
                                                 arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return __PyObject_Merge(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
}


// object.__new__()
static int
__PyCallable_Check(PyObject *arg, void *addr)
{
    if (PyCallable_Check(arg)) {
        *(PyObject **)addr = arg;
        return 1;
    }
    PyErr_Format(PyExc_TypeError,
                 "argument 1 must be a callable, not %.200s",
                 Py_TYPE(arg)->tp_name);
    return 0;
}

static PyObject *
__PyObject_New(PyObject *reduce)
{
    PyObject *callable, *args;
    PyObject *setstatearg = Py_None, *extendarg = Py_None, *updatearg = Py_None;
    PyObject *self = NULL;

    if (
        PyArg_ParseTuple(reduce, "O&O!|OOO",
                         __PyCallable_Check, &callable, &PyTuple_Type, &args,
                         &setstatearg, &extendarg, &updatearg) &&
        (self = PyObject_CallObject(callable, args)) &&
        (
         (setstatearg != Py_None && __PyObject_SetState(self, setstatearg)) ||
         (extendarg != Py_None && __PyObject_Extend(self, extendarg)) ||
         (updatearg != Py_None && __PyObject_Update(self, updatearg))
        )
       ) {
        Py_CLEAR(self);
    }
    return self;
}


static PyObject *
__unpack_instance(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    PyObject *result = NULL, *reduce = NULL;

    if ((reduce = __unpack_msg(msg, off))) {
        result = __PyObject_New(reduce);
        Py_DECREF(reduce);
    }
    return result;
}


/* -------------------------------------------------------------------------- */

#define __unpack_size__(m, o, s) \
    ((buffer = __unpack_buffer(m, o, s)) ? __unpack_int##s##__(buffer) : -1)

#define __unpack_size(t, m, o, s) \
    (((size = __unpack_size__(m, o, s)) < 0) ? NULL : __unpack_##t(m, o, size))


#define __SIZE_CASE__(T, t, s) \
    case (T | s): \
        result = __unpack_size(t, msg, off, s); \
        break;

#define SIZE_CASE(T, t) \
    __SIZE_CASE__(T, t, 1) \
    __SIZE_CASE__(T, t, 2) \
    __SIZE_CASE__(T, t, 4) \
    __SIZE_CASE__(T, t, 8) \


static PyObject *
__unpack_msg(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = TYPE_INVALID;
    const char *buffer = NULL;
    Py_ssize_t size = -1;
    PyObject *result = NULL;

    switch ((type = __unpack_type(msg, off))) {
        case TYPE_INVALID:
            if (!PyErr_Occurred()) {
                PyErr_Format(PyExc_TypeError,
                             "invalid type: '0x%02x'", type);
            }
            break;
        case TYPE_INT1:
            result = __unpack_int(msg, off, 1);
            break;
        case TYPE_INT2:
            result = __unpack_int(msg, off, 2);
            break;
        case TYPE_INT4:
            result = __unpack_int(msg, off, 4);
            break;
        case TYPE_INT8:
            result = __unpack_int(msg, off, 8);
            break;
        case TYPE_UINT:
            result = __unpack_uint(msg, off, 8);
            break;
        case TYPE_FLOAT:
            result = __unpack_float(msg, off, 8);
            break;
        case TYPE_COMPLEX:
            result = __unpack_complex(msg, off, 16);
            break;
        case TYPE_NONE:
            result = __Py_INCREF(Py_None);
            break;
        case TYPE_TRUE:
            result = __Py_INCREF(Py_True);
            break;
        case TYPE_FALSE:
            result = __Py_INCREF(Py_False);
            break;
        SIZE_CASE(TYPE_STR, str)
        SIZE_CASE(TYPE_BYTES, bytes)
        SIZE_CASE(TYPE_BYTEARRAY, bytearray)
        SIZE_CASE(TYPE_TUPLE, tuple)
        SIZE_CASE(TYPE_LIST, list)
        SIZE_CASE(TYPE_DICT, dict)
        SIZE_CASE(TYPE_SET, set)
        SIZE_CASE(TYPE_FROZENSET, frozenset)
        SIZE_CASE(TYPE_CLASS, class)
        SIZE_CASE(TYPE_SINGLETON, singleton)
        SIZE_CASE(TYPE_INSTANCE, instance)
        default:
            PyErr_Format(PyExc_TypeError,
                         "unknown type: '0x%02x'", type);
            break;
    }
    return result;
}


static PyObject *
__size(Py_buffer *msg)
{
    Py_ssize_t size = -1, len = msg->len;
    const char * buf = msg->buf;

    switch (len) {
        case 1:
            size = __unpack_int1__(buf);
            break;
        case 2:
            size = __unpack_int2__(buf);
            break;
        case 4:
            size = __unpack_int4__(buf);
            break;
        case 8:
            size = __unpack_int8__(buf);
            break;
        default:
            return PyErr_Format(PyExc_ValueError,
                                "invalid buffer len: %zd", len);
    }
    return PyLong_FromSsize_t(size);
}


/* --------------------------------------------------------------------------
   module
   -------------------------------------------------------------------------- */

/* pack.register() */
static PyObject *
pack_register(PyObject *module, PyObject *obj)
{
    module_state *state = NULL;

    if (!(state = _PyModule_GetState(module)) ||
        __register_object(state->registry, obj)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


/* pack.pack() */
static PyObject *
pack_pack(PyObject *module, PyObject *obj)
{
    PyObject *msg = NULL;

    if ((msg = __new_msg()) && __pack_object(msg, obj)) {
        Py_CLEAR(msg);
    }
    return msg;
}


/* pack.encode() */
static PyObject *
pack_encode(PyObject *module, PyObject *obj)
{
    PyObject *result = NULL, *msg = NULL;

    if ((msg = __new_msg())) {
        result = __pack_encode(msg, obj);
        Py_DECREF(msg);
    }
    return result;
}


/* pack.unpack() */
static PyObject *
pack_unpack(PyObject *module, PyObject *args)
{
    PyObject *result = NULL;
    Py_buffer msg;
    Py_ssize_t off = 0;

    if (PyArg_ParseTuple(args, "y*:unpack", &msg)) {
        result = __unpack_msg(&msg, &off);
        PyBuffer_Release(&msg);
    }
    return result;
}


/* pack.size() */
static PyObject *
pack_size(PyObject *module, PyObject *args)
{
    PyObject *result = NULL;
    Py_buffer msg;

    if (PyArg_ParseTuple(args, "y*:size", &msg)) {
        result = __size(&msg);
        PyBuffer_Release(&msg);
    }
    return result;
}


/* pack_def.m_methods */
static PyMethodDef pack_m_methods[] = {
    {"register", (PyCFunction)pack_register, METH_O,       "register(obj)"},
    {"pack",     (PyCFunction)pack_pack,     METH_O,       "pack(obj) -> msg"},
    {"encode",   (PyCFunction)pack_encode,   METH_O,       "encode(obj) -> msg"},
    {"unpack",   (PyCFunction)pack_unpack,   METH_VARARGS, "unpack(msg) -> obj"},
    {"size",     (PyCFunction)pack_size,     METH_VARARGS, "size(msg) -> int"},
    {NULL} /* Sentinel */
};


/* pack_def.m_traverse */
static int
pack_m_traverse(PyObject *module, visitproc visit, void *arg)
{
    module_state *state = NULL;

    if (!(state = _PyModule_GetState(module))) {
        return -1;
    }
    Py_VISIT(state->registry);
    return 0;
}


/* pack_def.m_clear */
static int
pack_m_clear(PyObject *module)
{
    module_state *state = NULL;

    if (!(state = _PyModule_GetState(module))) {
        return -1;
    }
    Py_CLEAR(state->registry);
    return 0;
}


/* pack_def.m_free */
static void
pack_m_free(PyObject *module)
{
    pack_m_clear(module);
}


/* pack_def */
static PyModuleDef pack_def = {
    PyModuleDef_HEAD_INIT,
    .m_name = "ippc.pack",
    .m_doc = "ippc.pack module",
    .m_size = sizeof(module_state),
    .m_methods = pack_m_methods,
    .m_traverse = (traverseproc)pack_m_traverse,
    .m_clear = (inquiry)pack_m_clear,
    .m_free = (freefunc)pack_m_free,
};


/* get module state */
static module_state *
_module_get_state(void)
{
    return (module_state *)_PyModuleDef_GetState(&pack_def);
}


static inline int
_module_state_init(PyObject *module)
{
    module_state *state = NULL;

    if (
        !(state = _PyModule_GetState(module)) ||
        !(state->registry = PyDict_New()) ||
        __register_object(state->registry, Py_NotImplemented) ||
        __register_object(state->registry, Py_Ellipsis)
       ) {
        return -1;
    }
    return 0;
}


static inline int
_module_init(PyObject *module)
{
    if (
        _module_state_init(module) ||
        PyModule_AddStringConstant(module, "__version__", PKG_VERSION)
       ) {
        return -1;
    }
    return 0;
}


/* module initialization */
PyMODINIT_FUNC
PyInit_pack(void)
{
    PyObject *module = NULL;

    if ((module = PyState_FindModule(&pack_def))) {
        Py_INCREF(module);
    }
    else if ((module = PyModule_Create(&pack_def)) && _module_init(module)) {
        Py_CLEAR(module);
    }
    return module;
}

