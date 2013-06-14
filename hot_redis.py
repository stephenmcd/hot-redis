
import collections
import operator
import time
import uuid
from Queue import Empty as QueueEmpty, Full as QueueFull
import redis


client = redis.Redis()
lua_scripts = {}

def load_lua_scripts():
    with open("bit.lua", "r") as f:
        luabit = f.read()
    with open("atoms.lua", "r") as f:
        for func in f.read().strip().split("function "):
            if func:
                name, code = func.split("\n", 1)
                name = name.split("(")[0].strip()
                code = code.rsplit("end", 1)[0].strip()
                if name in ("number_and", "number_or", "number_xor",
                            "number_lshift", "number_rshift"):
                    code = luabit + code
                lua_scripts[name] = client.register_script(code)

load_lua_scripts()


def value_left(self, value):
    return value.value if isinstance(value, self.__class__) else value


def value_right(self, value):
    return self if isinstance(value, self.__class__) else self.value


def op_left(op):
    def method(self, value):
        return op(self.value, value_left(self, value))
    return method


def op_right(op):
    def method(self, value):
        return op(value_left(self, value), value_right(self, value))
    return method


def inplace(method_name):
    def method(self, value):
        getattr(self, method_name)(value_left(self, value))
        return self
    return method


class Base(object):

    def __init__(self, value=None, key=None):
        self.key = key or str(uuid.uuid4())
        if value:
            self.value = value

    __eq__ = op_left(operator.eq)
    __lt__ = op_left(operator.lt)
    __le__ = op_left(operator.le)
    __gt__ = op_left(operator.gt)
    __ge__ = op_left(operator.ge)

    def __repr__(self):
        bits = (self.__class__.__name__, repr(self.value), self.key)
        return "%s(%s, '%s')" % bits

    def __getattr__(self, name):
        return self._dispatch(name)

    def _dispatch(self, name):
        try:
            func = getattr(client, name)
        except AttributeError:
            pass
        else:
            return lambda *a, **k: func(self.key, *a, **k)
        try:
            func = lua_scripts[name]
        except KeyError:
            pass
        else:
            return lambda *a, **k: func(keys=[self.key], args=a, **k)
        try:
            func = getattr(self.value, name)
        except KeyError:
            pass
        else:
            return func
        raise AttributeError(name)


class Bitwise(Base):

    __and__       = op_left(operator.and_)
    __or__        = op_left(operator.or_)
    __xor__       = op_left(operator.xor)
    __lshift__    = op_left(operator.lshift)
    __rshift__    = op_left(operator.rshift)
    __rand__      = op_right(operator.and_)
    __ror__       = op_right(operator.or_)
    __rxor__      = op_right(operator.xor)
    __rlshift__   = op_right(operator.lshift)
    __rrshift__   = op_right(operator.rshift)


class Sequential(Base):

    __add__       = op_left(operator.add)
    __mul__       = op_left(operator.mul)
    __radd__      = op_right(operator.add)
    __rmul__      = op_right(operator.mul)


class Numeric(Base):

    __add__       = op_left(operator.add)
    __sub__       = op_left(operator.sub)
    __mul__       = op_left(operator.mul)
    __div__       = op_left(operator.div)
    __floordiv__  = op_left(operator.floordiv)
    __truediv__   = op_left(operator.truediv)
    __mod__       = op_left(operator.mod)
    __divmod__    = op_left(divmod)
    __pow__       = op_left(operator.pow)
    __radd__      = op_right(operator.add)
    __rsub__      = op_right(operator.sub)
    __rmul__      = op_right(operator.mul)
    __rdiv__      = op_right(operator.div)
    __rtruediv__  = op_right(operator.truediv)
    __rfloordiv__ = op_right(operator.floordiv)
    __rmod__      = op_right(operator.mod)
    __rdivmod__   = op_right(divmod)
    __rpow__      = op_right(operator.pow)
    __iadd__      = inplace("incr")
    __isub__      = inplace("decr")
    __imul__      = inplace("number_multiply")
    __idiv__      = inplace("number_divide")
    __ifloordiv__ = inplace("number_floordiv")
    __imod__      = inplace("number_mod")
    __ipow__      = inplace("number_pow")


class List(Sequential):

    @property
    def value(self):
        return self[:]

    @value.setter
    def value(self, value):
        self.extend(value)

    __iadd__ = inplace("extend")
    __imul__ = inplace("list_multiply")

    def __len__(self):
        return self.llen()

    def __setitem__(self, i, value):
        try:
            self.lset(i, value)
        except redis.exceptions.ResponseError:
            raise IndexError

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start if i.start is not None else 0
            stop = i.stop if i.stop is not None else 0
            return self.lrange(start, stop - 1)
        item = self.lindex(i)
        if item is None:
            raise IndexError
        return item

    def __delitem__(self, i):
        self.pop(i)

    def __iter__(self):
        return iter(self.value)

    def append(self, value):
        self.extend([value])

    def extend(self, l):
        self.rpush(*l)

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
        return self.value.index(value)

    def count(self, value):
        return self.value.count(value)

    def sort(self, reverse=False):
        self._dispatch("sort")(desc=reverse, store=self.key, alpha=True)


class Set(Bitwise):

    @property
    def value(self):
        return self.smembers()

    @value.setter
    def value(self, value):
        self.update(value)

    def _all_redis(self, values):
        return all([isinstance(value, self.__class__) for value in values])

    def _to_keys(self, values):
        return [value.key for value in values]

    __iand__ = inplace("intersection_update")
    __ior__  = inplace("update")
    __ixor__ = inplace("symmetric_difference_update")
    __isub__ = inplace("difference_update")
    __rsub__ = op_right(operator.sub)

    def __and__(self, value):
        return self.intersection(value)

    def __or__(self, value):
        return self.union(value)

    def __xor__(self, value):
        return self.symmetric_difference(value)

    def __sub__(self, value):
        return self.difference(value)

    def __len__(self):
        return self.scard()

    def __contains__(self, value):
        return self.sismember(value)

    def __iter__(self):
        return iter(self.value)

    def add(self, value):
        self.update([value])

    def update(self, *values):
        self.sadd(*reduce(operator.or_, values))

    def pop(self):
        return self.spop()

    def clear(self):
        self.delete()

    def remove(self, value):
        if self.srem(value) == 0:
            raise KeyError(value)

    def discard(self, value):
        try:
            self.remove(value)
        except KeyError:
            pass

    def intersection(self, *values):
        if self._all_redis(values):
            return self.sinter(*self._to_keys(values))
        else:
            return reduce(operator.and_, (self.value,) + values)

    def intersection_update(self, *values):
        if self._all_redis(values):
            self.sinterstore(self.key, *self._to_keys(values))
        else:
            values = list(reduce(operator.and_, values))
            self.set_intersection_update(*values)
        return self

    def union(self, *values):
        if self._all_redis(values):
            return self.sunion(*self._to_keys(values))
        else:
            return reduce(operator.or_, (self.value,) + values)

    def difference(self, *values):
        if self._all_redis(values):
            return self.sdiff(*self._to_keys(values))
        else:
            return reduce(operator.sub, (self.value,) + values)

    def difference_update(self, *values):
        if self._all_redis(values):
            self.sdiffstore(self.key, *self._to_keys(values))
        else:
            all_values = [str(uuid.uuid4())]
            for value in values:
                all_values.extend(value)
                all_values.append(all_values[0])
            self.set_difference_update(*all_values)
        return self

    def symmetric_difference(self, value):
        if isinstance(value, self.__class__):
            return set(self.set_symmetric_difference("return", value.key))
        else:
            return self.value ^ value

    def symmetric_difference_update(self, value):
        if isinstance(value, self.__class__):
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


class Dict(Base):

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

    def __len__(self):
        return self.hlen()

    def __contains__(self, name):
        return self.hexists(name)

    def __iter__(self):
        return self.iterkeys()

    def __setitem__(self, name, value):
        self.hset(name, value)

    def __getitem__(self, name):
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __delitem__(self, name):
        if self.hdel(name) == 0:
            raise KeyError(name)

    def update(self, value):
        self.hmset(value)

    def keys(self):
        return self.hkeys()

    def values(self):
        return self.hvals()

    def items(self):
        return self.value.items()

    def iterkeys(self):
        return iter(self.keys())

    def itervalues(self):
        return iter(self.values())

    def iteritems(self):
        return iter(self.items())

    def setdefault(self, name, value=None):
        if self.hsetnx(name, value) == 1:
            return value
        else:
            return self.get(name)

    def get(self, name, default=None):
        value = self.hget(name)
        return value if value is not None else default

    def has_key(self, name):
        return name in self

    def copy(self):
        return self.__class__(self.value)

    def clear(self):
        self.delete()

    @classmethod
    def fromkeys(cls, *args):
        if len(args) == 1:
            args += ("",)
        return cls({}.fromkeys(*args))


class String(Sequential):

    @property
    def value(self):
        return self.get()

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    __iadd__ = inplace("append")
    __imul__ = inplace("string_multiply")

    def __len__(self):
        return self.strlen()

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start if i.start is not None else 0
            stop = i.stop
        else:
            start = i
            stop = None
        if stop is not None and stop < start + len(value):
            self.string_setitem(start, stop, value)
        else:
            self.setrange(start, value)

    def __getitem__(self, i):
        if not isinstance(i, slice):
            i = slice(i, i + 1)
        start = i.start if i.start is not None else 0
        stop = i.stop if i.stop is not None else 0
        value = self.getrange(start, stop - 1)
        if not value:
            raise IndexError
        return value

    def __iter__(self):
        return iter(self.value)


class ImmutableString(String):

    def __iadd__(self, s):
        self.key = self.__class__(self + s).key
        return self

    def __imul__(self, i):
        self.key = self.__class__(self * i).key
        return self

    def __setitem__(self, i):
        raise TypeError


class Int(Numeric, Bitwise):

    @property
    def value(self):
        return int(float(self.get()))

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    __iand__    = inplace("number_and")
    __ior__     = inplace("number_or")
    __ixor__    = inplace("number_xor")
    __ilshift__ = inplace("number_lshift")
    __irshift__ = inplace("number_rshift")


class Float(Numeric):

    @property
    def value(self):
        return float(self.get())

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    __iadd__ = inplace("incrbyfloat")

    def __isub__(self, f):
        self.incrbyfloat(f * -1)
        return self


class Queue(List):

    def __init__(self, value=None, key=None, maxsize=0):
        super(Queue, self).__init__(value=value, key=key)
        self.maxsize = maxsize

    @property
    def queue(self):
        return self

    def qsize(self):
        return len(self)

    def empty(self):
        return self.qsize() == 0

    def full(self):
        return self.maxsize > 0 and self.qsize() >= self.maxsize

    def put(self, item, block=True, timeout=None):
        if self.maxsize == 0:
            self.append(item)
        else:
            if not block:
                timeout = 0
            start = time.time()
            while True:
                if self.queue_put(item, self.maxsize):
                    break
                if timeout is not None and time.time() - start >= timeout:
                    raise QueueFull
                time.sleep(.1)

    def put_nowait(self, item):
        return self.put(item, block=False)

    def get(self, block=True, timeout=None):
        if block:
            item = self.blpop(timeout=timeout)
            if item is not None:
                item = item[1]
        else:
            item = self.pop()
        if item is None:
            raise QueueEmpty
        return item

    def get_nowait(self):
        return self.get(item, block=False)

    def join(self):
        while not self.empty():
            sleep(.1)


class LifoQueue(Queue):

    def append(self, item):
        self.lpush(item)


class DefaultDict(Dict):

    def __init__(self, default_factory, *args, **kwargs):
        self.default_factory = default_factory
        super(DefaultDict, self).__init__(*args, **kwargs)

    def __getitem__(self, name):
        return self.setdefault(name, self.default_factory())


class Counter(Dict):

    def __init__(self, value=None, key=None, **kwargs):
        super(Counter, self).__init__(key=key)
        self.update(value=value, **kwargs)

    @property
    def value(self):
        value = super(Counter, self).value
        kwargs = dict([(k, int(v)) for k, v in value.items()])
        return collections.Counter(**kwargs)

    __add__  = op_left(operator.add)
    __sub__  = op_left(operator.sub)
    __and__  = op_left(operator.and_)
    __or__   = op_left(operator.or_)
    __radd__ = op_right(operator.add)
    __rsub__ = op_right(operator.sub)
    __rand__ = op_right(operator.and_)
    __ror__  = op_right(operator.or_)
    __iadd__ = inplace("update")
    __isub__ = inplace("subtract")
    __iand__ = inplace("intersection_update")
    __ior__  = inplace("union_update")

    def __delitem__(self, name):
        try:
            super(Counter, self).__delitem__(name)
        except KeyError:
            pass

    def __repr__(self):
        bits = (self.__class__.__name__, repr(dict(self.value)), self.key)
        return "%s(%s, '%s')" % bits

    def values(self):
        values = super(Counter, self).values()
        return [int(v) for v in values]

    def get(self, name, default=None):
        value = self.hget(name)
        return int(value) if value is not None else default

    def _merge(self, value=None, **kwargs):
        if value:
            try:
                value.iteritems
            except AttributeError:
                for k in value:
                    kwargs[k] = kwargs.get(k, 0) + 1
            else:
                for k in value:
                    kwargs[k] = kwargs.get(k, 0) + value[k]
        return kwargs.items()

    def _flatten(self, value, **kwargs):
        for k, v in self._merge(value, **kwargs):
            yield k
            yield v

    def _update(self, value, multiplier, **kwargs):
        for k, v in self._merge(value, **kwargs):
            self.hincrby(k, v * multiplier)

    def update(self, value=None, **kwargs):
        self._update(value, 1, **kwargs)

    def subtract(self, value=None, **kwargs):
        self._update(value, -1, **kwargs)

    def intersection_update(self, value=None, **kwargs):
        self.counter_update("min", *self._flatten(value, **kwargs))

    def union_update(self, value=None, **kwargs):
        self.counter_update("max", *self._flatten(value, **kwargs))

    def elements(self):
        for k, count in self.iteritems():
            for i in range(count):
                yield k

    def most_common(self, n=None):
        values = sorted(self.iteritems(), key=lambda v: v[1], reverse=True)
        if n:
            values = values[:n]
        return [v[0] for v in values]

collections.MutableMapping.register(Counter)
