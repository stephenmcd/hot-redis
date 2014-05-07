#!/usr/bin/env python

import collections
import time
import Queue
import unittest

import hot_redis


keys = []



def base_wrapper(init):
    def wrapper(*args, **kwargs):
        init(*args, **kwargs)
        keys.append(args[0].key)


    return wrapper



hot_redis.Base.__init__ = base_wrapper(hot_redis.Base.__init__)


class BaseTestCase(unittest.TestCase):
    def tearDown(self):
        client = hot_redis.default_client()
        while keys:
            client.delete(keys.pop())


class LuaMultiMethodsTests(BaseTestCase):
    def setUp(self):
        self.longMessage = True

    def test_rank_lists_by_length(self):
        lengths = (61., 60., 5., 4., 3., 2., 1.)
        _keys = [("l%d" % length) for length in lengths]
        keys_with_lengths = zip(_keys, lengths)
        lists = [
            hot_redis.List(range(int(length)), key=key)
            for key, length
            in keys_with_lengths
        ]
        client = hot_redis.default_client()

        calls = [
            (lambda i: [x for x in i], 'iter'),
            (lambda i: i[:], '[:]'),
            (lambda i: i[3:4], '[3:4]'),
            (lambda i: i[-4:3], '[-4:3]'),
            (lambda i: i[2:], '[2:]'),
            (lambda i: i[:2], '[:2]'),
            #empty),
            (lambda i: i[2:1], '[2:1]'),
            (lambda i: i[-1:-2], '[-1:-2]')
        ]

        for call, code in calls:
            expected = call(keys_with_lengths)
            result = call(client.rank_lists_by_length(*_keys))
            self.assertEquals(
                result,
                expected,
                "Results not equal when called with call={code}, "
                "got result={result}, expected={expected}".format(
                    code=code, result=result,
                    expected=expected
                )
            )

        with self.assertRaises(ValueError):
            client.rank_lists_by_length('only_one_key'),

    def test_rank_sets_by_cardinality(self):
        cardinalities = (61., 60., 5., 4., 3., 2., 1.)
        _keys = [("l%d" % card) for card in cardinalities]
        keys_with_cardinalities = zip(_keys, cardinalities)
        sets = [
            hot_redis.Set(range(int(card)), key=key)
            for key, card
            in keys_with_cardinalities
        ]
        client = hot_redis.default_client()

        calls = [
            (lambda i: [x for x in i], 'iter'),
            (lambda i: i[:], '[:]'),
            (lambda i: i[3:4], '[3:4]'),
            (lambda i: i[-4:3], '[-4:3]'),
            (lambda i: i[2:], '[2:]'),
            (lambda i: i[:2], '[:2]'),
            #empty),
            (lambda i: i[2:1], '[2:1]'),
            (lambda i: i[-1:-2], '[-1:-2]')
        ]

        for call, code in calls:
            expected = call(keys_with_cardinalities)
            result = call(client.rank_sets_by_cardinality(*_keys))
            self.assertEquals(
                result,
                expected,
                "Results not equal when called with call={code}, "
                "got result={result}, expected={expected}".format(
                    code=code, result=result,
                    expected=expected
                )
            )

        with self.assertRaises(ValueError):
            client.rank_lists_by_length('only_one_key'),


class ListTests(BaseTestCase):
class ListTests(BaseTestCase):
    def test_value(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEquals(hot_redis.List(a), a)

    def test_empty(self):
        self.assertEquals(hot_redis.List(), [])

    def test_iter(self):
        a = ["wagwaan", "hot", "skull"]
        for i, x in enumerate(hot_redis.List(a)):
            self.assertEquals(x, a[i])

    def test_add(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        self.assertEquals(a + b, hot_redis.List(a) + hot_redis.List(b))
        self.assertEquals(a + b, hot_redis.List(a) + b)
        c = hot_redis.List(a)
        d = hot_redis.List(b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = 9000
        self.assertEquals(a * i, hot_redis.List(a) * i)
        b *= i
        self.assertEquals(a * i, b)

    def test_len(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEquals(len(a), len(hot_redis.List(a)))

    def test_get(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        self.assertEquals(a[4], b[4])
        self.assertEquals(a[3:12], b[3:12])
        self.assertEquals(a[:-5], b[:-5])
        self.assertEquals(a[:2], b[:2])
        self.assertEquals(a[2:], b[2:])
        self.assertEquals(a[:], b[:])
        self.assertRaises(IndexError, lambda: b[len(b)])

    def test_set(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = "popcaan"
        a[1] = i
        self.assertNotEquals(a, b)
        b[1] = i
        self.assertEquals(a, b)
        # todo: slice

    def test_del(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        del a[1]
        self.assertNotEquals(a, b)
        del b[1]
        self.assertEquals(a, b)
        # todo: slice?

    def test_len(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        self.assertEquals(len(a), len(b))

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
        self.assertEquals(a, c)

    def test_append(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = "popcaan"
        a.append(i)
        b.append(i)
        self.assertEquals(a, b)

    def test_insert(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        i = "popcaan"
        a.insert(1, i)
        b.insert(1, i)
        self.assertEquals(a, b)

    def test_pop(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        a.pop()
        b.pop()
        self.assertEquals(a, b)
        a.pop(0)
        b.pop(0)
        self.assertEquals(a, b)
        a.pop(-1)
        b.pop(-1)
        self.assertEquals(a, b)
        a.pop(20)
        b.pop(20)
        self.assertEquals(a, b)

    def test_reverse(self):
        a = ["wagwaan", "hot", "skull"]
        b = hot_redis.List(a)
        a.reverse()
        b.reverse()
        self.assertEquals(a, b)

    def test_index(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        c = "wagwaan"
        self.assertEquals(a.index(c), b.index(c))
        self.assertRaises(ValueError, lambda: b.index("popcaan"))

    def test_count(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        self.assertEquals(a.count("wagwaan"), b.count("wagwaan"))
        self.assertEquals(a.count("popcaan"), b.count("popcaan"))

    def test_sort(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = hot_redis.List(a)
        a.sort()
        b.sort()
        self.assertEquals(a, b)
        a.sort(reverse=True)
        b.sort(reverse=True)
        self.assertEquals(a, b)


class SetTests(BaseTestCase):
    def test_value(self):
        a = set(["wagwaan", "hot", "skull"])
        self.assertEquals(hot_redis.Set(a), a)

    def test_empty(self):
        self.assertEquals(hot_redis.Set(), set())

    def test_add(self):
        a = set(["wagwaan", "hot", "skull"])
        b = hot_redis.Set(a)
        i = "popcaan"
        a.add(i)
        b.add(i)
        self.assertEquals(b, a)

    def test_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = set(["rap", "dot", "mom"])
        d = hot_redis.Set(a)
        a.update(b, c)
        d.update(b, c)
        self.assertEquals(d, a)

    def test_pop(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = a.pop()
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)

    def test_clear(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        a.clear()
        self.assertEquals(len(a), 0)

    def test_remove(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.remove(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(KeyError, lambda: a.remove("popcaan"))

    def test_discard(self):
        a = hot_redis.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.discard(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertEquals(a.discard("popcaan"), None)

    def test_len(self):
        a = set(["wagwaan", "hot", "skull"])
        b = hot_redis.Set(a)
        self.assertEquals(len(a), len(b))

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
        self.assertEquals(a.intersection(b), d.intersection(b))
        self.assertEquals(e, d.intersection(b, c))
        self.assertEquals(e, d.intersection(hot_redis.Set(b), c))
        self.assertEquals(e, d.intersection(b, hot_redis.Set(c)))
        self.assertEquals(e,
                          d.intersection(hot_redis.Set(b), hot_redis.Set(c)))

    def test_intersection_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.intersection_update(b)
        e = hot_redis.Set(a)
        e.intersection_update(b)
        self.assertEquals(e, d)
        d = a.copy()
        d.intersection_update(b, c)
        e = hot_redis.Set(a)
        e.intersection_update(b, c)
        self.assertEquals(e, d)
        e = hot_redis.Set(a)
        e.intersection_update(hot_redis.Set(b), c)
        self.assertEquals(e, d)
        e = hot_redis.Set(a)
        e.intersection_update(b, hot_redis.Set(c))
        self.assertEquals(e, d)
        e = hot_redis.Set(a)
        e.intersection_update(hot_redis.Set(b), hot_redis.Set(c))
        self.assertEquals(e, d)

    def test_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = hot_redis.Set(a)
        e = a.difference(b, c)
        self.assertEquals(a.difference(b), d.difference(b))
        self.assertEquals(e, d.difference(b, c))
        self.assertEquals(e, d.difference(hot_redis.Set(b), c))
        self.assertEquals(e, d.difference(b, hot_redis.Set(c)))
        self.assertEquals(e, d.difference(hot_redis.Set(b), hot_redis.Set(c)))

    def test_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.difference_update(b)
        e = hot_redis.Set(a)
        e.difference_update(b)
        self.assertEquals(e, d)
        d = a.copy()
        d.difference_update(b, c)
        e = hot_redis.Set(a)
        e.difference_update(b, c)
        self.assertEquals(e, d)
        e = hot_redis.Set(a)
        e.difference_update(hot_redis.Set(b), c)
        self.assertEquals(e, d)
        e = hot_redis.Set(a)
        e.difference_update(b, hot_redis.Set(c))
        self.assertEquals(e, d)
        e = hot_redis.Set(a)
        e.difference_update(hot_redis.Set(b), hot_redis.Set(c))
        self.assertEquals(e, d)

    def test_symmetric_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = hot_redis.Set(a)
        d = a.symmetric_difference(b)
        self.assertEquals(d, c.symmetric_difference(b))
        self.assertEquals(d, c.symmetric_difference(hot_redis.Set(b)))
        self.assertEquals(d, a.symmetric_difference(hot_redis.Set(b)))

    def test_symmetric_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = a.copy()
        c.difference_update(b)
        d = hot_redis.Set(a)
        d.difference_update(b)
        self.assertEquals(d, c)
        d = hot_redis.Set(a)
        d.difference_update(hot_redis.Set(b))
        self.assertEquals(d, c)

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
        self.assertEquals(a > b, c > d)
        self.assertEquals(a < b, c < d)
        self.assertEquals(a > b, c > b)
        self.assertEquals(a < b, c < b)
        self.assertEquals(a >= b, c >= d)
        self.assertEquals(a <= b, c <= d)
        self.assertEquals(a >= b, c >= b)
        self.assertEquals(a <= b, c <= b)
        self.assertEquals(a.issubset(b), c.issubset(d))
        self.assertEquals(a.issuperset(b), c.issuperset(d))
        self.assertEquals(a.issubset(b), c.issubset(b))
        self.assertEquals(a.issuperset(b), c.issuperset(b))


class DictTests(BaseTestCase):
    def test_value(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertEquals(hot_redis.Dict(a), a)

    def test_empty(self):
        self.assertEquals(hot_redis.Dict(), {})

    def test_update(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = {"wagwaan": "hotskull", "nba": "hangtime"}
        c = hot_redis.Dict(a)
        a.update(b)
        c.update(b)
        self.assertEquals(a, c)

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
        self.assertEquals(e, d)
        self.assertEquals(b[c], d)
        self.assertEquals(a.setdefault(c, d), e)
        e = b.setdefault(c, c)
        self.assertEquals(e, d)
        self.assertEquals(a.setdefault(c, c), e)

    def test_get(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = hot_redis.Dict(a)
        self.assertEquals(a["wagwaan"], b["wagwaan"])
        self.assertEquals(a.get("wagwaan"), b.get("wagwaan"))
        self.assertRaises(KeyError, lambda: b["hotskull"])
        self.assertEquals(a.get("hotskull"), b.get("hotskull"))
        self.assertEquals(a.get("hotskull", "don"), b.get("hotskull", "don"))
        self.assertNotEquals(a.get("hotskull", "don"), b.get("hotskull", "x"))

    def test_set(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        a["wagwaan"] = "hotskull"
        self.assertEquals(a["wagwaan"], "hotskull")

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
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = hot_redis.Dict(a)
        self.assertIn("wagwaan", a)
        self.assertNotIn("hotskull", a)

    def test_copy(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        b = a.copy()
        self.assertEquals(type(a), type(b))
        self.assertNotEquals(a.key, b.key)

    def test_clear(self):
        a = hot_redis.Dict({"wagwaan": "popcaan", "flute": "don"})
        a.clear()
        self.assertEquals(len(a), 0)

    def test_fromkeys(self):
        a = ["wagwaan", "hot", "skull"]
        b = "popcaan"
        c = hot_redis.Dict.fromkeys(a)
        self.assertItemsEqual(a, c.keys())
        ccc = c
        self.assertFalse(c["wagwaan"])
        c = hot_redis.Dict.fromkeys(a, b)
        self.assertEquals(c["wagwaan"], b)

    def test_defaultdict(self):
        a = "wagwaan"
        b = "popcaan"
        c = hot_redis.DefaultDict(lambda: b)
        self.assertEquals(c[a], b)
        c[b] += a
        self.assertEquals(c[b], b + a)


class StringTests(BaseTestCase):
    def test_value(self):
        a = "wagwaan"
        self.assertEquals(hot_redis.String(a), a)

    def test_empty(self):
        self.assertEquals(hot_redis.String(), "")

    def test_add(self):
        a = "wagwaan"
        b = "hotskull"
        c = hot_redis.String(a)
        d = hot_redis.String(b)
        self.assertEquals(a + b, hot_redis.String(a) + hot_redis.String(b))
        self.assertEquals(a + b, hot_redis.String(a) + b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = "wagwaan"
        b = hot_redis.String(a)
        i = 9000
        self.assertEquals(a * i, hot_redis.String(a) * i)
        b *= i
        self.assertEquals(a * i, b)

    def test_len(self):
        a = "wagwaan"
        self.assertEquals(len(a), len(hot_redis.String(a)))

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
                self.assertEquals(d, c)

    def test_get(self):
        a = "wagwaan hotskull"
        b = hot_redis.String(a)
        self.assertEquals(a[4], b[4])
        self.assertEquals(a[3:12], b[3:12])
        self.assertEquals(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

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
        self.assertEquals(a, c)
        self.assertEquals(a, d)
        self.assertEquals(c.key, keyC)
        self.assertNotEquals(d.key, keyD)
        keyD = d.key
        i = 9000
        a *= i
        c *= i
        d *= i
        self.assertEquals(a, c)
        self.assertEquals(a, d)
        self.assertEquals(c.key, keyC)
        self.assertNotEquals(d.key, keyD)

        def immutable_set():
            d[0] = b

        self.assertRaises(TypeError, immutable_set)


class IntTests(BaseTestCase):
    def test_value(self):
        a = 420
        self.assertEquals(hot_redis.Int(a), a)

    def test_empty(self):
        self.assertEquals(hot_redis.Int(), 0)

    def test_add(self):
        a = 420
        b = 9000
        self.assertEquals(a + b, hot_redis.Int(a) + hot_redis.Int(b))
        self.assertEquals(a + b, hot_redis.Int(a) + b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = 420
        b = hot_redis.Int(a)
        i = 9000
        self.assertEquals(a * i, hot_redis.Int(a) * i)
        b *= i
        self.assertEquals(a * i, b)

    def test_sub(self):
        a = 420
        b = 9000
        self.assertEquals(a - b, hot_redis.Int(a) - hot_redis.Int(b))
        self.assertEquals(a - b, hot_redis.Int(a) - b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d -= c
        c -= b
        self.assertEquals(a - b, c)
        self.assertEquals(b - a, d)

    def test_div(self):
        a = 420
        b = 9000
        self.assertEquals(a / b, hot_redis.Int(a) / hot_redis.Int(b))
        self.assertEquals(a / b, hot_redis.Int(a) / b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d /= c
        c /= b
        self.assertEquals(a / b, c)
        self.assertEquals(b / a, d)

    def test_mod(self):
        a = 420
        b = 9000
        self.assertEquals(a % b, hot_redis.Int(a) % hot_redis.Int(b))
        self.assertEquals(a % b, hot_redis.Int(a) % b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d %= c
        c %= b
        self.assertEquals(a % b, c)
        self.assertEquals(b % a, d)

    def test_pow(self):
        a = 4
        b = 20
        self.assertEquals(a ** b, hot_redis.Int(a) ** hot_redis.Int(b))
        self.assertEquals(a ** b, hot_redis.Int(a) ** b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d **= c
        c **= b
        self.assertEquals(a ** b, c)
        self.assertEquals(b ** a, d)

    def test_add(self):
        a = 420
        b = 9000
        self.assertEquals(a & b, hot_redis.Int(a) & hot_redis.Int(b))
        self.assertEquals(a & b, hot_redis.Int(a) & b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d &= c
        c &= b
        self.assertEquals(a & b, c)
        self.assertEquals(b & a, d)

    def test_or(self):
        a = 420
        b = 9000
        self.assertEquals(a | b, hot_redis.Int(a) | hot_redis.Int(b))
        self.assertEquals(a | b, hot_redis.Int(a) | b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d |= c
        c |= b
        self.assertEquals(a | b, c)
        self.assertEquals(b | a, d)

    def test_xor(self):
        a = 420
        b = 9000
        self.assertEquals(a ^ b, hot_redis.Int(a) ^ hot_redis.Int(b))
        self.assertEquals(a ^ b, hot_redis.Int(a) ^ b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d ^= c
        c ^= b
        self.assertEquals(a ^ b, c)
        self.assertEquals(b ^ a, d)

    def test_lshift(self):
        a = 4
        b = 20
        self.assertEquals(a << b, hot_redis.Int(a) << hot_redis.Int(b))
        self.assertEquals(a << b, hot_redis.Int(a) << b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d <<= c
        c <<= b
        self.assertEquals(a << b, c)
        self.assertEquals(b << a, d)

    def test_rshift(self):
        a = 9000
        b = 4
        self.assertEquals(a >> b, hot_redis.Int(a) >> hot_redis.Int(b))
        self.assertEquals(a >> b, hot_redis.Int(a) >> b)
        c = hot_redis.Int(a)
        d = hot_redis.Int(b)
        d >>= c
        c >>= b
        self.assertEquals(a >> b, c)
        self.assertEquals(b >> a, d)


class FloatTests(BaseTestCase):
    def test_value(self):
        a = 420.666
        self.assertAlmostEqual(hot_redis.Float(a), a)

    def test_empty(self):
        self.assertEquals(hot_redis.Int(), .0)

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

    def test_pow(self):
        a = 4.666
        b = 20.666
        c = 4
        d = 2
        self.assertAlmostEqual(a ** b,
                               hot_redis.Float(a) ** hot_redis.Float(b))
        self.assertAlmostEqual(a ** b, hot_redis.Float(a) ** b)
        e = hot_redis.Float(a)
        f = hot_redis.Float(b)
        f **= c
        e **= d
        self.assertAlmostEqual(a ** d, e)
        self.assertAlmostEqual(b ** c, f)


class QueueTests(BaseTestCase):
    def test_put(self):
        a = "wagwaan"
        b = "hotskull"
        q = hot_redis.Queue(maxsize=2)
        q.put(a)
        self.assertIn(a, q)
        q.put(b)
        self.assertIn(b, q)
        self.assertRaises(Queue.Full, lambda: q.put("popcaan", block=False))
        start = time.time()
        timeout = 2
        try:
            q.put("popcaan", timeout=timeout)
        except Queue.Full:
            pass
        self.assertTrue(time.time() - start >= timeout)

    def test_get(self):
        a = "wagwaan"
        b = "hotskull"
        q = hot_redis.Queue()
        q.put(a)
        q.put(b)
        self.assertEquals(a, q.get())
        self.assertNotIn(a, q)
        self.assertEquals(b, q.get())
        self.assertNotIn(b, q)
        self.assertRaises(Queue.Empty, lambda: q.get(block=False))
        start = time.time()
        timeout = 2
        try:
            q.get(timeout=timeout)
        except Queue.Empty:
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
        self.assertEquals(q.qsize(), 0)
        q.put("wagwaan")
        self.assertEquals(q.qsize(), 1)
        q.put("hotskull")
        self.assertEquals(q.qsize(), 2)
        q.get()
        self.assertEquals(q.qsize(), 1)

    def test_lifo_queue(self):
        a = "wagwaan"
        b = "hotskull"
        q = hot_redis.LifoQueue()
        q.put(a)
        q.put(b)
        self.assertEquals(b, q.get())
        self.assertNotIn(b, q)
        self.assertEquals(a, q.get())
        self.assertNotIn(a, q)

    def test_set_queue(self):
        a = "wagwaan"
        q = hot_redis.SetQueue()
        q.put(a)
        self.assertEquals(q.qsize(), 1)
        q.put(a)
        self.assertEquals(q.qsize(), 1)
        self.assertEquals(q.get(), a)
        self.assertEquals(q.qsize(), 0)


class CounterTest(object):

    def test_value(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = hot_redis.MultiSet(a)
        e = collections.Counter(**b)
        f = hot_redis.MultiSet(**b)
        self.assertEquals(d, c)
        self.assertEquals(f, e)

    def test_empty(self):
        self.assertEquals(hot_redis.MultiSet(), collections.Counter())

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
        self.assertEquals(c.get("a"), d.get("a"))
        self.assertEquals(c.get("flute", "don"), d.get("flute", "don"))
        self.assertEquals(e.get("hot"), f.get("hot"))
        self.assertEquals(e.get("skull"), f.get("skull"))
        self.assertEquals(e.get("flute", "don"), e.get("flute", "don"))

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
        self.assertItemsEqual(c.elements(), d.elements())
        self.assertItemsEqual(e.elements(), f.elements())

    def test_most_common(self):
        a = "wanwaa"
        b = collections.Counter(a)
        c = hot_redis.MultiSet(a)
        d = 420
        self.assertEqual(c.most_common(d), b.most_common(d))
        self.assertEqual(c.most_common(), b.most_common())


if __name__ == "__main__":
    unittest.main()
