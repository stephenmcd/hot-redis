
import operator
import uuid
import redis


client = redis.Redis()
lua_scripts = {}

def load_lua_scripts():
    with open("atoms.lua", "r") as f:
        for func in f.read().strip().split("function "):
            if not func:
                continue
            name, code = func.split("\n", 1)
            name = name.split("(")[0].strip()
            code = code.rsplit("end", 1)[0].strip()
            lua_scripts[name] = client.register_script(code)

load_lua_scripts()


value_left  = lambda a, b: b.value if isinstance(b, a.__class__) else b
value_right = lambda a, b: a if isinstance(b, a.__class__) else a.value
op_left     = lambda op: lambda a, b: op(a.value, value_left(a, b))
op_right    = lambda op: lambda a, b: op(value_left(a, b), value_right(a, b))


class Comparative(object):
    __eq__        = op_left(operator.eq)
    __lt__        = op_left(operator.lt)
    __le__        = op_left(operator.le)
    __gt__        = op_left(operator.gt)
    __ge__        = op_left(operator.ge)

class Binary(object):
    __and__       = op_left(operator.and_)
    __rand__      = op_right(operator.and_)
    __or__        = op_left(operator.or_)
    __ror__       = op_right(operator.or_)
    __xor__       = op_left(operator.xor)
    __rxor__      = op_right(operator.xor)

class Commutative(object):
    __add__       = op_left(operator.add)
    __radd__      = op_right(operator.add)
    __mul__       = op_left(operator.mul)
    __rmul__      = op_right(operator.mul)

class Arithemtic(Binary, Commutative):
    __sub__       = op_left(operator.sub)
    __rsub__      = op_right(operator.sub)
    __floordiv__  = op_left(operator.floordiv)
    __rfloordiv__ = op_right(operator.floordiv)
    __mod__       = op_left(operator.mod)
    __rmod__      = op_right(operator.mod)
    __divmod__    = op_left(divmod)
    __rdivmod__   = op_right(divmod)
    __pow__       = op_left(operator.pow)
    __rpow__      = op_right(operator.pow)
    __lshift__    = op_left(operator.lshift)
    __rlshift__   = op_right(operator.lshift)
    __rshift__    = op_left(operator.rshift)
    __rrshift__   = op_right(operator.rshift)


class Base(Comparative):

    def __init__(self, value=None, key=None):
        self.key = key or str(uuid.uuid4())
        if value:
            self.value = value

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

    def __getattr__(self, name):
        return self._dispatch(name)

    def __repr__(self):
        value = repr(self.value)
        return "%s(%s, '%s')" % (self.__class__.__name__, value, self.key)

    def __iter__(self):
        return iter(self.value)


class List(Base, Commutative):

    @property
    def value(self):
        return self[:]

    @value.setter
    def value(self, value):
        self.extend(value)

    def __iadd__(self, l):
        self.extend(value_left(self, l))
        return self

    def __imul__(self, i):
        self.list_multiply(i)
        return self

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


class Set(Base, Binary):

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

    def __and__(self, value):
        return self.intersection(value)

    def __iand___(self, value):
        self.intersection_update(value)
        return self

    def __or__(self, value):
        return self.union(value)

    def __ior___(self, value):
        self.update(value)
        return self

    def __xor__(self, value):
        return self.symmetric_difference(value)

    def __ixor__(self, value):
        self.symmetric_difference_update(value)
        return self

    def __sub__(self, value):
        return self.difference(value)

    def __isub__(self, value):
        self.difference_update(value)
        return self

    __rsub__ = op_right(operator.sub)

    def __len__(self):
        return self.scard()

    def __contains__(self, value):
        return self.sismember(value)

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
            return self[name]

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


class String(Base, Commutative):

    @property
    def value(self):
        return self.get()

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    def __iadd__(self, s):
        self.append(value_left(self, s))
        return self

    def __imul__(self, i):
        self.string_multiply(i)
        return self

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


class ImmutableString(String):

    def __iadd__(self, s):
        self.key = self.__class__(self + s).key
        return self

    def __imul__(self, i):
        self.key = self.__class__(self * i).key
        return self

    def __setitem__(self, i):
        raise TypeError


class Int(Base, Arithemtic):

    @property
    def value(self):
        return int(self.get())

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    def __isub__(self, i):
        self.decr(i)
        return self

    def __iadd__(self, i):
        self.incr(i)
        return self

    def __imul__(self, i):
        return self



