
from contextlib import contextmanager
from operator import and_, or_, sub, xor
from uuid import uuid4
from redis import Redis
from redis.exceptions import ResponseError


redis = Redis()
_pipeline = None
client = lambda: _pipeline or redis

@contextmanager
def pipeline():
    global _pipeline
    _pipeline = redis.pipeline()
    yield
    _pipeline.execute()
    _pipeline = None

lua_funcs = {}
with open("atoms.lua", "r") as f:
    for func in f.read().strip().split("function "):
        if not func:
            continue
        name, code = func.split("\n", 1)
        name = name.split("(")[0].strip()
        code = code.rsplit("end", 1)[0].strip()
        lua_funcs[name] = redis.register_script(code)


class Base(object):

    def __init__(self, value=None, key=None):
        self.key = key or str(uuid4())

    def __del__(self):
        self.delete()

    def _proxy(self, name):
        try:
            func = getattr(client(), name)
        except AttributeError:
            pass
        else:
            return lambda *a, **k: func(self.key, *a, **k)
        try:
            func = lua_funcs[name]
        except KeyError:
            pass
        else:
            return lambda *a, **k: func(keys=[self.key], args=a, **k)
        raise AttributeError(name)

    def __getattr__(self, name):
        return self._proxy(name)

    def __repr__(self):
        return "%s(%s, '%s')" % (self.__class__.__name__, self.value, self.key)


class Iterable(Base):

    def __init__(self, value=None, key=None):
        super(Iterable, self).__init__(value, key)
        self.type = None
        if self._is_many(value):
            self.value = value

    def _proxy(self, name):
        func = super(Iterable, self)._proxy(name)
        def wrapper(*args, **kwargs):
            value = func(*args, **kwargs)
            if value is not None:
                return self._set_type(value)
        return wrapper

    def _to_value(self, value):
        if type(value) == type(self):
            return value.value
        return value

    def _is_many(self, value):
        return hasattr(value, "__iter__")

    def _set_type(self, value):
        if self.type == str:
            return value
        elif self._is_many(value):
            typed_value = map(self._set_type, value)
            if type(typed_value) != type(value):
                # Everything other than lists.
                typed_value = type(value)(typed_value)
            return typed_value
        elif self.type:
            return self.type(value)
        return value

    def _check_type(self, value):
        if self._is_many(value):
            map(self._check_type, value)
        else:
            t = type(value)
            if not self.type:
                self.type = t
            elif t != self.type:
                raise TypeError("%s != %s" % (t, self.type))

    def __eq__(self, value):
        return self.value == self._to_value(value)

    def __iter__(self):
        return iter(self.value)

    def __lt__(self, value):
        return self.value < self._to_value(value)

    def __le__(self, value):
        return self.value <= self._to_value(value)

    def __gt__(self, value):
        return self.value > self._to_value(value)

    def __ge__(self, value):
        return self.value >= self._to_value(value)


class List(Iterable):

    @property
    def value(self):
        return self[:]

    @value.setter
    def value(self, value):
        self.extend(value)

    def __add__(self, l):
        return List(self.value + self._to_value(l))

    def __iadd__(self, l):
        self.extend(self._to_value(l))
        return self

    def __mul__(self, i):
        return List(self.value * i)

    def __imul__(self, i):
        self.list_multiply(i)
        return self

    def __len__(self):
        return self.llen()

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start if i.start is not None else 0
            stop = i.stop if i.stop is not None else 0
            return self.lrange(start, stop - 1)
        item = self.lindex(i)
        if item is None:
            raise IndexError
        return item

    def __setitem__(self, i, value):
        self._check_type(value)
        try:
            self.lset(i, value)
        except ResponseError:
            raise IndexError

    def __delitem__(self, i):
        self.pop(i)

    def extend(self, l):
        self._check_type(l)
        self.rpush(*l)

    def append(self, value):
        self.extend([value])

    def insert(self, i, value):
        self._check_type(value)
        self.list_insert(i, value)

    def pop(self, i=-1):
        if i == -1:
            return self.rpop()
        elif i == 0:
            return self.lpop()
        else:
            return self.list_pop(i)

    def reverse(self):
        self.list_reverse()

    def index(self, value):
        return self.value.index(value)

    def count(self, value):
        return self.value.count(value)

    def sort(self, reverse=False):
        self._proxy("sort")(desc=reverse, store=self.key)


class Set(Iterable):

    @property
    def value(self):
        return self.smembers()

    @value.setter
    def value(self, value):
        self.update(value)

    def _all_redis(self, values):
        return all([isinstance(value, Set) for value in values])

    def _reduce(self, op, values):
        checked_values = []
        for value in values:
            self._check_type(value)
            checked_values.append(set(map(self._to_value, value)))
        return reduce(op, checked_values)

    def _rop(self, op, value):
        return op(value, self if isinstance(value, Set) else self.value)

    def _to_keys(self, values):
        return [value.key for value in values]

    def add(self, value):
        self.update([value])

    def update(self, *values):
        self.sadd(*self._reduce(or_, values))

    def pop(self):
        return self.spop()

    def clear(self):
        self.delete()

    def remove(self, value):
        self._check_type(value)
        if self.srem(value) == 0:
            raise KeyError

    def discard(self, value):
        try:
            self.remove(value)
        except KeyError:
            pass

    def __len__(self):
        return self.scard()

    def __contains__(self, value):
        return self.sismember(value)

    def __and__(self, value):
        return self.intersection(value)

    def __iand___(self, value):
        self.intersection_update(value)
        return self

    def __rand__(self, value):
        return self._rop(and_, value)

    def intersection(self, *values):
        if self._all_redis(values):
            return self.sinter(*self._to_keys(values))
        else:
            return self._reduce(and_, (self.value,) + values)

    def intersection_update(self, *values):
        if self._all_redis(values):
            self.sinterstore(self.key, *self._to_keys(values))
        else:
            values = list(self._reduce(and_, values))
            self.set_intersection_update(*values)
        return self

    def __or__(self, value):
        return self.union(value)

    def __ior___(self, value):
        self.update(value)
        return self

    def __ror__(self, value):
        return self._rop(or_, value)

    def union(self, *values):
        if self._all_redis(values):
            return self.sunion(*self._to_keys(values))
        else:
            return self._reduce(or_, (self.value,) + values)

    def __sub__(self, value):
        return self.difference(value)

    def __isub__(self, value):
        self.difference_update(value)
        return self

    def __rsub__(self, value):
        return self._rop(sub, value)

    def difference(self, *values):
        if self._all_redis(values):
            return self.sdiff(*self._to_keys(values))
        else:
            return self._reduce(sub, (self.value,) + values)

    def difference_update(self, *values):
        if self._all_redis(values):
            self.sdiffstore(self.key, *self._to_keys(values))
        else:
            values = list(self._reduce(sub, values))
            self.set_difference_update(*values)
        return self

    def __xor__(self, value):
        return self.symmetric_difference(value)

    def __ixor__(self, value):
        self.symmetric_difference_update(value)
        return self

    def __rxor__(self, value):
        return self._rop(xor, value)

    def symmetric_difference(self, value):
        if isinstance(value, Set):
            return self.set_symmetric_difference("return", value.key)
        else:
            return self.value ^ value

    def symmetric_difference_update(self, value):
        if isinstance(value, Set):
            self.set_symmetric_difference("update", value.key)
        else:
            self.set_symmetric_difference("create", *value)
        return self

    def isdisjoint(self, value):
        return not self.intersection(value)

    def issubset(self, value):
        return self <= value

    def issuperset(self, value):
        return self >= value


class Dict(Iterable):

    @property
    def value(self):
        return self.hgetall()

    @value.setter
    def value(self, value):
        if not isinstance(value, dict):
            try:
                value = dict(value)
            except TypeError:
                value = None
        if value:
            self.update(value)

    def update(self, value):
        self.hmset(value)

    def keys(self):
        return self.hkeys()

    def values(self):
        return self.values()

    def items(self):
        return self.value.items()

    def setdefault(self, name, value=None):
        if self.hsetnx(name, value) == 1:
            return value

    def get(self, name, default=None):
        return self.hget(name) or default

    def __getitem__(self, name):
        value = self.get(name)
        if value is None:
            raise KeyError

    def __setitem__(self, name, value):
        self.hset(name)

    def __delitem__(self, name):
        self.hdel(name)
