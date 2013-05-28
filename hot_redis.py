
from contextlib import contextmanager
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

    def proxy(self, name):
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
        raise AttributeError

    def __getattr__(self, name):
        return self.proxy(name)

    def __repr__(self):
        return "%s" % getattr(self, "value", "")


class List(Base):

    def __init__(self, value=None, key=None):
        super(List, self).__init__(value, key)
        self.type = None
        if not isinstance(value, list):
            try:
                list(value)
            except TypeError:
                value = None
        if value:
            self.extend(value)

    def proxy(self, name):
        func = super(List, self).proxy(name)
        def wrapper(*args, **kwargs):
            value = func(*args, **kwargs)
            if value is not None:
                if isinstance(value, list):
                    return map(self.type, value)
                return self.type(value)
        return wrapper

    def check_type(self, value):
        t = type(value)
        if not self.type:
            self.type = t
        elif t != self.type:
            raise TypeError("%s != %s" % (t, self.type))

    @property
    def value(self):
        return self[:]

    def __iter__(self):
        return iter(self.value)

    def __add__(self, l):
        return List(self.value + l)

    def __iadd__(self, l):
        self.extend(l)
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
            stop = i.stop if i.stop is not None else -1
            return self.lrange(start, stop)
        item = self.lindex(i)
        if item is None:
            raise IndexError
        return item

    def __setitem__(self, i, value):
        self.check_type(value)
        try:
            self.lset(i, value)
        except ResponseError:
            raise IndexError

    def __delitem__(self, i):
        self.pop(i)

    def extend(self, l):
        map(self.check_type, l)
        self.rpush(*l)

    def append(self, value):
        self.extend([value])

    def insert(self, i, value):
        self.check_type(value)
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
        self.proxy("sort")(desc=reverse, store=self.key)


class Set(Base):

    def __init__(self, value=None, key=None):
        super(Set, self).__init__(value, key)
        self.type = None
        try:
            iter(value)
            self.update(value)
        except TypeError:
            pass

    @property
    def value(self):
        return self.smembers()

    def add(self, value):
        self.update([value])

    def update(self, value):
        self.sadd(*value)

    def pop(self):
        return self.spop()

    def clear(self):
        del self

    def remove(self):
        if self.srem(value) == 0:
            raise KeyError

    def discard(self, value):
        self.srem(value)

    def __len__(self, value):
        return self.scard()

    def __contains__(self, value):
        return self.sismember(value)

    def __and__(self, value):
        raise NotImplemented
    def __iand__(self, value):
        raise NotImplemented
    def __rand__(self, value):
        raise NotImplemented
    def intersection(self, value):
        raise NotImplemented
    def intersection_update(self, value):
        raise NotImplemented

    def __or__(self, value):
        raise NotImplemented
    def __ior__(self, value):
        raise NotImplemented
    def __ror__(self, value):
        raise NotImplemented
    def union(self, value):
        raise NotImplemented

    def __xor__(self, value):
        raise NotImplemented
    def symmetric_difference(self, value):
        raise NotImplemented
    def symmetric_difference_update(self, value):
        raise NotImplemented

    def __sub__(self, value):
        raise NotImplemented
    def __isub__(self, value):
        raise NotImplemented
    def __rsub__(self, value):
        raise NotImplemented
    def difference(self, value):
        raise NotImplemented

    def issubset(self, value):
        raise NotImplemented
    def issuperset(self, value):
        raise NotImplemented
    def isdisjoint(self, value):
        raise NotImplemented


class Dict(Base):

    def __init__(self, value=None, key=None):
        super(Dict, self).__init__(value, key)
        if not isinstance(value, dict):
            try:
                value = dict(value)
            except TypeError:
                value = None
        if value:
            self.update(value)

    @property
    def value(self):
        return self.hgetall()

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
