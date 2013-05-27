
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
with open("functions.lua", "r") as f:
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

    def __getattr__(self, name):
        try:
            func = getattr(client(), name)
        except AttributeError:
            pass
        else:
            return lambda *args: func(self.key, *args)
        try:
            func = lua_funcs[name]
        except KeyError:
            pass
        else:
            return lambda *args: func(keys=[self.key], args=args)
        raise AttributeError


class List(Base):

    def __init__(self, value=None, key=None):
        super(List, self).__init__(value, key)
        try:
            iter(value)
        except TypeError:
            value = None
        else:
            if not isinstance(value, list):
                value = list(value)
        if value:
            self.extend(value)

    def __iter__(self):
        return iter(self[:])

    def __add__(self, l):
        return List(self[:] + l)

    def __iadd__(self, l):
        self.extend(l)
        return self

    def __mul__(self, i):
        return List(self[:] * i)

    def __imul__(self, i):
        self.list_multiply(i)
        return self

    def __repr__(self):
        return "%s" % self[:]

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
        try:
            self.lset(i, value)
        except ResponseError:
            raise IndexError

    def __delitem__(self, i):
        self.pop(i)

    def extend(self, l):
        self.rpush(*l)

    def append(self, value):
        self.extend([value])

    def insert(self, i, value):
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
        self[:].index(value)

    def count(self, value):
        self[:].count(value)

