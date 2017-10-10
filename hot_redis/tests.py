#!/usr/bin/env python

import collections
import os
import time
import unittest
import hot_redis


# Env var specifying we're dealing with a server that doesn't support
# Lua, like an old version of Redis, or some kind of Redis clone.
TEST_NO_LUA = os.environ.get("HOT_REDIS_TEST_NO_LUA") == "1"

# Env var specifying the precision for assertAlmostEqual - we may want
# to define this for alternate Lua implementations, like LuaJ.
TEST_PRECISION = int(os.environ.get("HOT_REDIS_TEST_PRECISION", 0)) or None

keys = []

def base_wrapper(init):
    def wrapper(*args, **kwargs):
        init(*args, **kwargs)
        keys.append(args[0].key)
    return wrapper

hot_redis.Base.__init__ = base_wrapper(hot_redis.Base.__init__)
if TEST_NO_LUA:
    hot_redis.HotClient._create_lua_method = lambda *args, **kwargs: None


class BaseTestCase(unittest.TestCase):

    def tearDown(self):
        client = hot_redis.default_client()
        while keys:
            client.delete(keys.pop())

    # Removed in Python 3.
    def assertItemsEqual(self, a, b):
        self.assertEqual(sorted(a), sorted(b))

    # Configurable precision.
    def assertAlmostEqual(self, a, b):
        kwargs = {"places": TEST_PRECISION}
        return super(BaseTestCase, self).assertAlmostEqual(a, b, **kwargs)


class ListTests(BaseTestCase):

    def test_initial(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        c = hot_redis.List(a, key=b.key)
        self.assertItemsEqual(a, c)

    def test_value(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEqual(hot_redis.List(a), a)

    def test_empty(self):
        self.assertEqual(hot_redis.List(), [])

    def test_iter(self):
        a = ["wagwaan", "hot", "skull"]
        for i, x in enumerate(hot_redis.List(a)):
            self.assertEqual(x, a[i])

    def test_add(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        self.assertEqual(a + b, hot_redis.List(a) + hot_redis.List(b))
        self.assertEqual(a + b, hot_redis.List(a) + b)
        c = hot_redis.List(a)
        d = hot_redis.List(b)
        d += c
        c += b
        self.assertEqual(a + b, c)
        self.assertEqual(b + a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mul(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = 10
        self.assertEqual(a * i, hot_redis.List(a) * i)
        b *= i
        self.assertEqual(a * i, b)

    def test_len(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEqual(len(a), len(hot_redis.List(a)))

    def test_get(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        self.assertEqual(a[4], b[4])
        self.assertEqual(a[3:12], b[3:12])
        self.assertEqual(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

    def test_set(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = "popcaan"
        a[1] = i
        self.assertNotEqual(a, b)
        b[1] = i
        self.assertEqual(a, b)
        # todo: slice

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_del(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        del a[1]
        self.assertNotEqual(a, b)
        del b[1]
        self.assertEqual(a, b)
        # todo: slice?

    def test_contains(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        self.assertIn("wagwaan", a)
        self.assertNotIn("hotskull", a)

    def test_extend(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        c = hot_redis.List(a)
        a.extend(b)
        c.extend(b)
        self.assertEqual(a, c)

    def test_append(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = "popcaan"
        a.append(i)
        b.append(i)
        self.assertEqual(a, b)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_insert(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = "popcaan"
        a.insert(1, i)
        b.insert(1, i)
        self.assertEqual(a, b)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_pop(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        a.pop()
        b.pop()
        self.assertEqual(a, b)
        a.pop(0)
        b.pop(0)
        self.assertEqual(a, b)
        a.pop(-1)
        b.pop(-1)
        self.assertEqual(a, b)
        a.pop(20)
        b.pop(20)
        self.assertEqual(a, b)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_reverse(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        a.reverse()
        b.reverse()
        self.assertEqual(a, b)

    def test_index(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        c = "wagwaan"
        self.assertEqual(a.index(c), b.index(c))
        self.assertRaises(ValueError, lambda: b.index("popcaan"))

    def test_count(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        self.assertEqual(a.count("wagwaan"), b.count("wagwaan"))
        self.assertEqual(a.count("popcaan"), b.count("popcaan"))

    def test_sort(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        a.sort()
        b.sort()
        self.assertEqual(a, b)
        a.sort(reverse=True)
        b.sort(reverse=True)
        self.assertEqual(a, b)


class SetTests(BaseTestCase):

    def test_value(self):
        a = set(["wagwaan", "hot", "skull"])
        self.assertEqual(hot_redis.Set(a), a)

    def test_empty(self):
        self.assertEqual(hot_redis.Set(), set())

    def test_add(self):
        a = set(["wagwaan", "hot", "skull"])
        b = hot_redis.Set(a)
        i = "popcaan"
        a.add(i)
        b.add(i)
        self.assertEqual(b, a)

    def test_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = set(["rap", "dot", "mom"])
        d = hot_redis.Set(a)
        a.update(b, c)
        d.update(b, c)
        self.assertEqual(d, a)

    def test_pop(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = a.pop()
        self.assertEqual(len(a), i - 1)
        self.assertNotIn(b, a)

    def test_clear(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        a.clear()
        self.assertEqual(len(a), 0)

    def test_remove(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.remove(b)
        self.assertEqual(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(KeyError, lambda: a.remove("popcaan"))

    def test_discard(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.discard(b)
        self.assertEqual(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertEqual(a.discard("popcaan"), None)

    def test_len(self):
        a = set(["wagwaan", "hot", "skull"])
        b = hot_redis.Set(a)
        self.assertEqual(len(a), len(b))

    def test_contains(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        self.assertIn("wagwaan", a)
        self.assertNotIn("popcaan", a)

    def test_intersection(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = hot_redis.Set(a)
        e = a.intersection(b, c)
        self.assertEqual(a.intersection(b), d.intersection(b))
        self.assertEqual(e, d.intersection(b, c))
        self.assertEqual(e, d.intersection(hot_redis.Set(b), c))
        self.assertEqual(e, d.intersection(b, hot_redis.Set(c)))
        self.assertEqual(e, d.intersection(hot_redis.Set(b), hot_redis.Set(c)))

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_intersection_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.intersection_update(b)
        e = hot_redis.Set(a)
        e.intersection_update(b)
        self.assertEqual(e, d)
        d = a.copy()
        d.intersection_update(b, c)
        e = hot_redis.Set(a)
        e.intersection_update(b, c)
        self.assertEqual(e, d)
        e = hot_redis.Set(a)
        e.intersection_update(hot_redis.Set(b), c)
        self.assertEqual(e, d)
        e = hot_redis.Set(a)
        e.intersection_update(b, hot_redis.Set(c))
        self.assertEqual(e, d)
        e = hot_redis.Set(a)
        e.intersection_update(hot_redis.Set(b), hot_redis.Set(c))
        self.assertEqual(e, d)

    def test_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = hot_redis.Set(a)
        e = a.difference(b, c)
        self.assertEqual(a.difference(b), d.difference(b))
        self.assertEqual(e, d.difference(b, c))
        self.assertEqual(e, d.difference(hot_redis.Set(b), c))
        self.assertEqual(e, d.difference(b, hot_redis.Set(c)))
        self.assertEqual(e, d.difference(hot_redis.Set(b), hot_redis.Set(c)))

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.difference_update(b)
        e = hot_redis.Set(a)
        e.difference_update(b)
        self.assertEqual(e, d)
        d = a.copy()
        d.difference_update(b, c)
        e = hot_redis.Set(a)
        e.difference_update(b, c)
        self.assertEqual(e, d)
        e = hot_redis.Set(a)
        e.difference_update(hot_redis.Set(b), c)
        self.assertEqual(e, d)
        e = hot_redis.Set(a)
        e.difference_update(b, hot_redis.Set(c))
        self.assertEqual(e, d)
        e = hot_redis.Set(a)
        e.difference_update(hot_redis.Set(b), hot_redis.Set(c))
        self.assertEqual(e, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_symmetric_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = hot_redis.Set(a)
        d = a.symmetric_difference(b)
        self.assertEqual(d, c.symmetric_difference(b))
        self.assertEqual(d, c.symmetric_difference(hot_redis.Set(b)))
        self.assertEqual(d, a.symmetric_difference(hot_redis.Set(b)))

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_symmetric_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = a.copy()
        c.difference_update(b)
        d = hot_redis.Set(a)
        d.difference_update(b)
        self.assertEqual(d, c)
        d = hot_redis.Set(a)
        d.difference_update(hot_redis.Set(b))
        self.assertEqual(d, c)

    def test_disjoint(self):
        a = set(["wagwaan", "hot", "skull"])
        b = hot_redis.Set(a)
        c = hot_redis.Set(["wagwaan", "flute", "don"])
        d = set(["nba", "hang", "time"])
        e = hot_redis.Set(d)
        self.assertFalse(b.isdisjoint(a))
        self.assertFalse(b.isdisjoint(c))
        self.assertTrue(b.isdisjoint(d))
        self.assertTrue(b.isdisjoint(e))

    def test_cmp(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = hot_redis.Set(a)
        d = hot_redis.Set(b)
        self.assertEqual(a > b, c > d)
        self.assertEqual(a < b, c < d)
        self.assertEqual(a > b, c > b)
        self.assertEqual(a < b, c < b)
        self.assertEqual(a >= b, c >= d)
        self.assertEqual(a <= b, c <= d)
        self.assertEqual(a >= b, c >= b)
        self.assertEqual(a <= b, c <= b)
        self.assertEqual(a.issubset(b), c.issubset(d))
        self.assertEqual(a.issuperset(b), c.issuperset(d))
        self.assertEqual(a.issubset(b), c.issubset(b))
        self.assertEqual(a.issuperset(b), c.issuperset(b))


class DictTests(BaseTestCase):

    def test_value(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertEqual(hot_redis.Dict(a), a)

    def test_empty(self):
        self.assertEqual(hot_redis.Dict(), {})

    def test_update(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = {"wagwaan": "hotskull", "nba": "hangtime"}
        c = hot_redis.Dict(a)
        a.update(b)
        c.update(b)
        self.assertEqual(a, c)

    def test_iter(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(iter(a), iter(hot_redis.Dict(a)))

    def test_keys(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.keys(), hot_redis.Dict(a).keys())

    def test_values(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.values(), hot_redis.Dict(a).values())

    def test_items(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.items(), hot_redis.Dict(a).items())

    def test_setdefault(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = hot_redis.Dict(a)
        c = "nba"
        d = "hangtime"
        e = b.setdefault(c, d)
        self.assertEqual(e, d)
        self.assertEqual(b[c], d)
        self.assertEqual(a.setdefault(c, d), e)
        e = b.setdefault(c, c)
        self.assertEqual(e, d)
        self.assertEqual(a.setdefault(c, c), e)

    def test_get(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = hot_redis.Dict(a)
        self.assertEqual(a["wagwaan"], b["wagwaan"])
        self.assertEqual(a.get("wagwaan"), b.get("wagwaan"))
        self.assertRaises(KeyError, lambda: b["hotskull"])
        self.assertEqual(a.get("hotskull"), b.get("hotskull"))
        self.assertEqual(a.get("hotskull", "don"), b.get("hotskull", "don"))
        self.assertNotEqual(a.get("hotskull", "don"), b.get("hotskull", "x"))

    def test_set(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        a["wagwaan"] = "hotskull"
        self.assertEqual(a["wagwaan"], "hotskull")

    def test_del(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        del a["wagwaan"]
        self.assertRaises(KeyError, lambda: a["wagwaan"])
        def del_missing():
            del a["hotskull"]
        self.assertRaises(KeyError, del_missing)

    def test_len(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = hot_redis.Dict(a)
        self.assertEqual(len(a), len(b))

    def test_contains(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = hot_redis.Dict(a)
        self.assertIn("wagwaan", a)
        self.assertNotIn("hotskull", a)

    def test_copy(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        b = a.copy()
        self.assertEqual(type(a), type(b))
        self.assertNotEqual(a.key, b.key)

    def test_clear(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        a.clear()
        self.assertEqual(len(a), 0)

    def test_fromkeys(self):
        a = ["wagwaan", "hot", "skull"]
        b = "popcaan"
        c = hot_redis.Dict.fromkeys(a)
        self.assertItemsEqual(sorted(a), sorted(c.keys()))
        self.assertFalse(c["wagwaan"])
        c = hot_redis.Dict.fromkeys(a, b)
        self.assertEqual(c["wagwaan"], b)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_defaultdict(self):
        a = "wagwaan"
        b = "popcaan"
        c = hot_redis.DefaultDict(lambda: b)
        self.assertEqual(c[a], b)
        c[b] += a
        self.assertEqual(c[b], b + a)


class StringTests(BaseTestCase):

    def test_value(self):
        a = "wagwaan"
        self.assertEqual(hot_redis.String(a), a)

    def test_empty(self):
        self.assertEqual(hot_redis.String(), "")

    def test_add(self):
        a = "wagwaan"
        b = "hotskull"
        c = hot_redis.String(a)
        d = hot_redis.String(b)
        self.assertEqual(a + b, hot_redis.String(a) + hot_redis.String(b))
        self.assertEqual(a + b, hot_redis.String(a) + b)
        d += c
        c += b
        self.assertEqual(a + b, c)
        self.assertEqual(b + a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mul(self):
        a = "wagwaan"
        b = hot_redis.String(a)
        i = 9000
        self.assertEqual(a * i, hot_redis.String(a) * i)
        b *= i
        self.assertEqual(a * i, b)

    def test_len(self):
        a = "wagwaan"
        self.assertEqual(len(a), len(hot_redis.String(a)))

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_set(self):
        a = "wagwaan hotskull"
        b = "flute don"
        for i in range(0, len(b)):
            for j in range(i, len(b)):
                c = list(a)
                d = hot_redis.String(a)
                c[i:j] = list(b)
                d[i:j] = b
                c = "".join(c)
                self.assertEqual(d, c)

    def test_get(self):
        a = "wagwaan hotskull"
        b = hot_redis.String(a)
        self.assertEqual(a[4], b[4])
        self.assertEqual(a[3:12], b[3:12])
        self.assertEqual(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mutability(self):
        a = "wagwaan hotskull"
        b = "flute don"
        c = hot_redis.String(a)
        d = hot_redis.ImmutableString(a)
        keyC = c.key
        keyD = d.key
        a += b
        c += b
        d += b
        self.assertEqual(a, c)
        self.assertEqual(a, d)
        self.assertEqual(c.key, keyC)
        self.assertNotEqual(d.key, keyD)
        keyD = d.key
        i = 9000
        a *= i
        c *= i
        d *= i
        self.assertEqual(a, c)
        self.assertEqual(a, d)
        self.assertEqual(c.key, keyC)
        self.assertNotEqual(d.key, keyD)
        def immutable_set():
            d[0] = b
        self.assertRaises(TypeError, immutable_set)


class IntTests(BaseTestCase):

    def test_value(self):
        a = 420
        self.assertEqual(hot_redis.Int(a), a)

    def test_empty(self):
        self.assertEqual(hot_redis.Int(), 0)

    def test_set(self):
        a = 420
        b = hot_redis.Int()
        b.value = a
        self.assertEqual(b, a)
        b.value = 0
        self.assertEqual(b, 0)

    def test_add(self):
        a = 420
        b = 9000
        self.assertEqual(a + b, hot_redis.Int(a) + hot_redis.Int(b))
        self.assertEqual(a + b, hot_redis.Int(a) + b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d += c
        c += b
        self.assertEqual(a + b, c)
        self.assertEqual(b + a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mul(self):
        a = 420
        b = hot_redis.Int(a)
        i = 9000
        self.assertEqual(a * i, hot_redis.Int(a) * i)
        b *= i
        self.assertEqual(a * i, b)

    def test_sub(self):
        a = 420
        b = 9000
        self.assertEqual(a - b, hot_redis.Int(a) - hot_redis.Int(b))
        self.assertEqual(a - b, hot_redis.Int(a) - b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d -= c
        c -= b
        self.assertEqual(a - b, c)
        self.assertEqual(b - a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_div(self):
        a = 420
        b = 9000
        self.assertEqual(a / b, hot_redis.Int(a) / hot_redis.Int(b))
        self.assertEqual(a / b, hot_redis.Int(a) / b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d /= c
        c /= b
        self.assertEqual(a / b, c)
        self.assertEqual(b / a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mod(self):
        a = 420
        b = 9000
        self.assertEqual(a % b, hot_redis.Int(a) % hot_redis.Int(b))
        self.assertEqual(a % b, hot_redis.Int(a) % b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d %= c
        c %= b
        self.assertEqual(a % b, c)
        self.assertEqual(b % a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_pow(self):
        a = 4
        b = 20
        self.assertEqual(a ** b, hot_redis.Int(a) ** hot_redis.Int(b))
        self.assertEqual(a ** b, hot_redis.Int(a) ** b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d **= c
        c **= b
        self.assertEqual(a ** b, c)
        self.assertEqual(b ** a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_and(self):
        a = 420
        b = 9000
        self.assertEqual(a & b, hot_redis.Int(a) & hot_redis.Int(b))
        self.assertEqual(a & b, hot_redis.Int(a) & b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d &= c
        c &= b
        self.assertEqual(a & b, c)
        self.assertEqual(b & a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_or(self):
        a = 420
        b = 9000
        self.assertEqual(a | b, hot_redis.Int(a) | hot_redis.Int(b))
        self.assertEqual(a | b, hot_redis.Int(a) | b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d |= c
        c |= b
        self.assertEqual(a | b, c)
        self.assertEqual(b | a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_xor(self):
        a = 420
        b = 9000
        self.assertEqual(a ^ b, hot_redis.Int(a) ^ hot_redis.Int(b))
        self.assertEqual(a ^ b, hot_redis.Int(a) ^ b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d ^= c
        c ^= b
        self.assertEqual(a ^ b, c)
        self.assertEqual(b ^ a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_lshift(self):
        a = 4
        b = 20
        self.assertEqual(a << b, hot_redis.Int(a) << hot_redis.Int(b))
        self.assertEqual(a << b, hot_redis.Int(a) << b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d <<= c
        c <<= b
        self.assertEqual(a << b, c)
        self.assertEqual(b << a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_rshift(self):
        a = 9000
        b = 4
        self.assertEqual(a >> b, hot_redis.Int(a) >> hot_redis.Int(b))
        self.assertEqual(a >> b, hot_redis.Int(a) >> b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d >>= c
        c >>= b
        self.assertEqual(a >> b, c)
        self.assertEqual(b >> a, d)


class FloatTests(BaseTestCase):

    def test_value(self):
        a = 420.666
        self.assertAlmostEqual(hot_redis.Float(a), a)

    def test_empty(self):
        self.assertEqual(hot_redis.Int(), .0)

    def test_set(self):
        a = 420.666
        b = hot_redis.Float()
        b.value = a
        self.assertAlmostEqual(b, a)
        b.value = .0
        self.assertAlmostEqual(b, .0)

    def test_add(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a + b, hot_redis.Float(a) + hot_redis.Float(b))
        self.assertAlmostEqual(a + b, hot_redis.Float(a) + b)
        c = hot_redis.Float(a)
        d = hot_redis.Float(b)
        d += c
        c += b
        self.assertAlmostEqual(a + b, c)
        self.assertAlmostEqual(b + a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mul(self):
        a = 420.666
        b = hot_redis.Float(a)
        i = 9000.666
        self.assertAlmostEqual(a * i, hot_redis.Float(a) * i)
        b *= i
        self.assertAlmostEqual(a * i, b)

    def test_sub(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a - b, hot_redis.Float(a) - hot_redis.Float(b))
        self.assertAlmostEqual(a - b, hot_redis.Float(a) - b)
        c = hot_redis.Float(a)
        d = hot_redis.Float(b)
        d -= c
        c -= b
        self.assertAlmostEqual(a - b, c)
        self.assertAlmostEqual(b - a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_div(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a / b, hot_redis.Float(a) / hot_redis.Float(b))
        self.assertAlmostEqual(a / b, hot_redis.Float(a) / b)
        c = hot_redis.Float(a)
        d = hot_redis.Float(b)
        d /= c
        c /= b
        self.assertAlmostEqual(a / b, c)
        self.assertAlmostEqual(b / a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_mod(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a % b, hot_redis.Float(a) % hot_redis.Float(b))
        self.assertAlmostEqual(a % b, hot_redis.Float(a) % b)
        c = hot_redis.Float(a)
        d = hot_redis.Float(b)
        d %= c
        c %= b
        self.assertAlmostEqual(a % b, c)
        self.assertAlmostEqual(b % a, d)

    @unittest.skipIf(TEST_NO_LUA, "No Lua")
    def test_pow(self):
        a = 4.666
        b = 20.666
        c = 4
        d = 2
        self.assertAlmostEqual(a ** b, hot_redis.Float(a) ** hot_redis.Float(b))
        self.assertAlmostEqual(a ** b, hot_redis.Float(a) ** b)
        e = hot_redis.Float(a)
        f = hot_redis.Float(b)
        f **= c
        e **= d
        self.assertAlmostEqual(a ** d, e)
        self.assertAlmostEqual(b ** c, f)


@unittest.skipIf(TEST_NO_LUA, "No Lua")
class QueueTests(BaseTestCase):

    def test_put(self):
        a = "wagwaan"
        b = "hotskull"
        q = hot_redis.Queue(maxsize=2)
        q.put(a)
        self.assertIn(a, q)
        q.put(b)
        self.assertIn(b, q)
        self.assertRaises(hot_redis.queue.Full, lambda: q.put("popcaan", block=False))
        start = time.time()
        timeout = 2
        try:
            q.put("popcaan", timeout=timeout)
        except hot_redis.queue.Full:
            pass
        self.assertTrue(time.time() - start >= timeout)

    def test_get(self):
        a = "wagwaan"
        b = "hotskull"
        q = hot_redis.Queue()
        q.put(a)
        q.put(b)
        self.assertEqual(a, q.get())
        self.assertNotIn(a, q)
        self.assertEqual(b, q.get())
        self.assertNotIn(b, q)
        self.assertRaises(hot_redis.queue.Empty, lambda: q.get(block=False))
        start = time.time()
        timeout = 2
        try:
            q.get(timeout=timeout)
        except hot_redis.queue.Empty:
            pass
        self.assertTrue(time.time() - start >= timeout)

    def test_empty(self):
        q = hot_redis.Queue()
        self.assertTrue(q.empty())
        q.put("wagwaan")
        self.assertFalse(q.empty())
        q.get()
        self.assertTrue(q.empty())

    def test_full(self):
        q = hot_redis.Queue(maxsize=2)
        self.assertFalse(q.full())
        q.put("wagwaan")
        self.assertFalse(q.full())
        q.put("hotskull")
        self.assertTrue(q.full())
        q.get()
        self.assertFalse(q.full())

    def test_size(self):
        q = hot_redis.Queue()
        self.assertEqual(q.qsize(), 0)
        q.put("wagwaan")
        self.assertEqual(q.qsize(), 1)
        q.put("hotskull")
        self.assertEqual(q.qsize(), 2)
        q.get()
        self.assertEqual(q.qsize(), 1)

    def test_lifo_queue(self):
        a = "wagwaan"
        b = "hotskull"
        q = hot_redis.LifoQueue()
        q.put(a)
        q.put(b)
        self.assertEqual(b, q.get())
        self.assertNotIn(b, q)
        self.assertEqual(a, q.get())
        self.assertNotIn(a, q)

    def test_set_queue(self):
        a = "wagwaan"
        q = hot_redis.SetQueue()
        q.put(a)
        self.assertEqual(q.qsize(), 1)
        q.put(a)
        self.assertEqual(q.qsize(), 1)
        self.assertEqual(q.get(), a)
        self.assertEqual(q.qsize(), 0)


@unittest.skipIf(TEST_NO_LUA, "No Lua")
class CounterTests(BaseTestCase):

    def test_value(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        e = collections.Counter(**b)
        f = hot_redis.MultiSet(**b)
        self.assertEqual(d, c)
        self.assertEqual(f, e)

    def test_empty(self):
        self.assertEqual(hot_redis.MultiSet(), collections.Counter())

    def test_values(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        e = collections.Counter(**b)
        f = hot_redis.MultiSet(**b)
        self.assertItemsEqual(c.values(), d.values())
        self.assertItemsEqual(e.values(), f.values())

    def test_get(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        e = collections.Counter(**b)
        f = hot_redis.MultiSet(**b)
        self.assertEqual(c.get("a"), d.get("a"))
        self.assertEqual(c.get("flute", "don"), d.get("flute", "don"))
        self.assertEqual(e.get("hot"), f.get("hot"))
        self.assertEqual(e.get("skull"), f.get("skull"))
        self.assertEqual(e.get("flute", "don"), e.get("flute", "don"))

    def test_del(self):
        a = hot_redis.MultiSet("wagwaan")
        del a["hotskull"]

    def test_update(self):
        a = "wagwaan"
        b = {"hotskull": 420}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.update(hot_redis.MultiSet(a))
        d.update(hot_redis.MultiSet(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.update(collections.Counter(a))
        d.update(collections.Counter(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.update(a)
        d.update(a)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.update(b)
        d.update(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.update(**b)
        d.update(**b)
        self.assertEqual(d, c)

    def test_subtract(self):
        a = "wagwaan"
        b = {"hotskull": 420}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.subtract(hot_redis.MultiSet(a))
        d.subtract(hot_redis.MultiSet(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.subtract(collections.Counter(a))
        d.subtract(collections.Counter(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.subtract(a)
        d.subtract(a)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.subtract(b)
        d.subtract(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c.subtract(**b)
        d.subtract(**b)
        self.assertEqual(d, c)

    def test_intersection(self):
        a = "wagwaan"
        b = "flute don"
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c &= hot_redis.MultiSet(b)
        d &= hot_redis.MultiSet(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c &= collections.Counter(b)
        d &= collections.Counter(b)
        self.assertEqual(d, c)

    def test_union(self):
        a = "wagwaan"
        b = "flute don"
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c |= hot_redis.MultiSet(b)
        d |= hot_redis.MultiSet(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        c |= collections.Counter(b)
        d |= collections.Counter(b)
        self.assertEqual(d, c)

    def test_elements(self):
        a = "wagwaan"
        b = {"hotskull": 420}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        e = collections.Counter(**b)
        f = hot_redis.MultiSet(**b)
        self.assertItemsEqual(sorted(c.elements()), sorted(d.elements()))
        self.assertItemsEqual(sorted(e.elements()), sorted(f.elements()))

    def test_most_common(self):
        a = "wagwaan"
        b = collections.Counter(a)
        c = hot_redis.MultiSet(a)
        d = 420
        check = b.most_common(d)
        for i, e in enumerate(c.most_common(d)):
            self.assertEqual(e[1], check[i][1])
        check = b.most_common()
        for i, e in enumerate(c.most_common()):
            self.assertEqual(e[1], check[i][1])

@unittest.skipIf(TEST_NO_LUA, "No Lua")
class TransactionTests(BaseTestCase):

    def test_transaction(self):
        with_transaction = hot_redis.List([1])
        without_transaction = hot_redis.List(key=with_transaction.key,
                                             client=hot_redis.HotClient())
        with hot_redis.transaction():
            with_transaction.append(1)
            self.assertEqual(len(without_transaction), 1)
        self.assertEqual(len(without_transaction), 2)


@unittest.skipIf(TEST_NO_LUA, "No Lua")
class LockTests(BaseTestCase):

    def test_semaphore(self):
        semaphore = hot_redis.Semaphore()
        self.assertEqual(semaphore.acquire(), True)
        self.assertEqual(semaphore.release(), None)
        self.assertEqual(semaphore.release(), None)

    def test_bounded_semaphore(self):
        max_size = 2
        semaphore = hot_redis.BoundedSemaphore(value=max_size)
        self.assertEqual(semaphore.acquire(), True)
        self.assertEqual(semaphore.release(), None)
        with semaphore:
            with semaphore:
                self.assertEqual(semaphore.acquire(block=False), False)
        self.assertRaises(RuntimeError, semaphore.release)

    def test_lock(self):
        lock = hot_redis.Lock()
        self.assertEqual(lock.acquire(), True)
        self.assertEqual(lock.release(), None)
        with lock:
            self.assertEqual(lock.acquire(block=False), False)
        self.assertRaises(RuntimeError, lock.release)


if __name__ == "__main__":
    unittest.main()
