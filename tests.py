#!/usr/bin/env python

import unittest

from hot_redis import List, Set, Dict


class ListTests(unittest.TestCase):

    def test_value(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEquals(List(a), a)

    def test_iter(self):
        a = ["wagwaan", "hot", "skull"]
        for i, x in enumerate(List(a)):
            self.assertEquals(x, a[i])

    def test_add(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        self.assertEquals(a + b, List(a) + List(b))
        self.assertEquals(a + b, List(a) + b)

    def test_iadd(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        c = List(a)
        d = List(b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = ["wagwaan", "hot", "skull"]
        i = 9000
        self.assertEquals(a * i, List(a) * i)

    def test_imul(self):
        a = ["wagwaan", "hot", "skull"]
        b = List(a)
        i = 9000
        b *= i
        self.assertEquals(a * i, b)

    def test_len(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEquals(len(a), len(List(a)))

    def test_get(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = List(a)
        self.assertEquals(a[4], b[4])
        self.assertEquals(a[3:12], b[3:12])
        self.assertEquals(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

    def test_set(self):
        a = ["wagwaan", "hot", "skull"]
        b = List(a)
        i = "popcaan"
        a[1] = i
        self.assertNotEquals(a, b)
        b[1] = i
        self.assertEquals(a, b)
        # todo: slice

    def test_del(self):
        a = ["wagwaan", "hot", "skull"]
        b = List(a)
        del a[1]
        self.assertNotEquals(a, b)
        del b[1]
        self.assertEquals(a, b)
        # todo: slice?

    def test_extend(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        c = List(a)
        a.extend(b)
        c.extend(b)
        self.assertEquals(a, c)

    def test_append(self):
        a = ["wagwaan", "hot", "skull"]
        b = List(a)
        i = "popcaan"
        a.append(i)
        b.append(i)
        self.assertEquals(a, b)

    def test_insert(self):
        a = ["wagwaan", "hot", "skull"]
        b = List(a)
        i = "popcaan"
        a.insert(1, i)
        b.insert(1, i)
        self.assertEquals(a, b)

    def test_pop(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = List(a)
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
        b = List(a)
        a.reverse()
        b.reverse()
        self.assertEquals(a, b)

    def test_index(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = List(a)
        c = "wagwaan"
        self.assertEquals(a.index(c), b.index(c))
        self.assertRaises(ValueError, lambda: b.index("popcaan"))

    def test_count(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = List(a)
        self.assertEquals(a.count("wagwaan"), b.count("wagwaan"))
        self.assertEquals(a.count("popcaan"), b.count("popcaan"))

    def test_sort(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = List(a)
        a.sort()
        b.sort()
        self.assertEquals(a, b)
        a.sort(reverse=True)
        b.sort(reverse=True)
        self.assertEquals(a, b)


class SetTests(unittest.TestCase):

    def test_value(self):
        a = set(["wagwaan", "hot", "skull"])
        self.assertEquals(Set(a), a)

    def test_add(self):
        a = set(["wagwaan", "hot", "skull"])
        b = Set(a)
        i = "popcaan"
        a.add(i)
        b.add(i)
        self.assertEquals(b, a)

    def test_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = set(["rap", "dot", "mom"])
        d = Set(a)
        a.update(b, c)
        d.update(b, c)
        self.assertEquals(d, a)

    def test_pop(self):
        a = Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = a.pop()
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)

    def test_clear(self):
        a = Set(["wagwaan", "hot", "skull"])
        a.clear()
        self.assertEquals(len(a), 0)

    def test_remove(self):
        a = Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.remove(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(KeyError, lambda: a.remove("popcaan"))

    def test_discard(self):
        a = Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.discard(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertEquals(a.discard("popcaan"), None)

    def test_len(self):
        a = set(["wagwaan", "hot", "skull"])
        b = Set(a)
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = Set(["wagwaan", "hot", "skull"])
        self.assertIn("wagwaan", a)
        self.assertNotIn("popcaan", a)

    def test_intersection(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = Set(a)
        e = a.intersection(b, c)
        self.assertEquals(a.intersection(b), d.intersection(b))
        self.assertEquals(e, d.intersection(b, c))
        self.assertEquals(e, d.intersection(Set(b), c))
        self.assertEquals(e, d.intersection(b, Set(c)))
        self.assertEquals(e, d.intersection(Set(b), Set(c)))

    def test_intersection_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.intersection_update(b)
        e = Set(a)
        e.intersection_update(b)
        self.assertEquals(e, d)
        d = a.copy()
        d.intersection_update(b, c)
        e = Set(a)
        e.intersection_update(b, c)
        self.assertEquals(e, d)
        e = Set(a)
        e.intersection_update(Set(b), c)
        self.assertEquals(e, d)
        e = Set(a)
        e.intersection_update(b, Set(c))
        self.assertEquals(e, d)
        e = Set(a)
        e.intersection_update(Set(b), Set(c))
        self.assertEquals(e, d)

    def test_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = Set(a)
        e = a.difference(b, c)
        self.assertEquals(a.difference(b), d.difference(b))
        self.assertEquals(e, d.difference(b, c))
        self.assertEquals(e, d.difference(Set(b), c))
        self.assertEquals(e, d.difference(b, Set(c)))
        self.assertEquals(e, d.difference(Set(b), Set(c)))

    def test_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.difference_update(b)
        e = Set(a)
        e.difference_update(b)
        self.assertEquals(e, d)
        d = a.copy()
        d.difference_update(b, c)
        e = Set(a)
        e.difference_update(b, c)
        self.assertEquals(e, d)
        e = Set(a)
        e.difference_update(Set(b), c)
        self.assertEquals(e, d)
        e = Set(a)
        e.difference_update(b, Set(c))
        self.assertEquals(e, d)
        e = Set(a)
        e.difference_update(Set(b), Set(c))
        self.assertEquals(e, d)

    def test_symmetric_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = Set(a)
        d = a.symmetric_difference(b)
        self.assertEquals(d, c.symmetric_difference(b))
        self.assertEquals(d, c.symmetric_difference(Set(b)))
        self.assertEquals(d, a.symmetric_difference(Set(b)))

    def test_symmetric_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = a.copy()
        c.difference_update(b)
        d = Set(a)
        d.difference_update(b)
        self.assertEquals(d, c)
        d = Set(a)
        d.difference_update(Set(b))
        self.assertEquals(d, c)

    def test_disjoint(self):
        a = set(["wagwaan", "hot", "skull"])
        b = Set(a)
        c = Set(["wagwaan", "flute", "don"])
        d = set(["nba", "hang", "time"])
        e = Set(d)
        self.assertFalse(b.isdisjoint(a))
        self.assertFalse(b.isdisjoint(c))
        self.assertTrue(b.isdisjoint(d))
        self.assertTrue(b.isdisjoint(e))

    def test_cmp(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = Set(a)
        d = Set(b)
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


class DictTests(unittest.TestCase):

    def test_value(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertEquals(Dict(a), a)

    def test_update(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = {"wagwaan": "hotskull", "nba": "hangtime"}
        c = Dict(a)
        a.update(b)
        c.update(b)
        self.assertEquals(a, c)

    def test_iter(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(iter(a), iter(Dict(a)))

    def test_keys(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.keys(), Dict(a).keys())

    def test_values(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.values(), Dict(a).values())

    def test_items(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.items(), Dict(a).items())

    def test_setdefault(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = Dict(a)
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
        b = Dict(a)
        self.assertEquals(a["wagwaan"], b["wagwaan"])
        self.assertEquals(a.get("wagwaan"), b.get("wagwaan"))
        self.assertRaises(KeyError, lambda: b["hotskull"])
        self.assertEquals(a.get("hotskull"), b.get("hotskull"))
        self.assertEquals(a.get("hotskull", "don"), b.get("hotskull", "don"))
        self.assertNotEquals(a.get("hotskull", "don"), b.get("hotskull", "x"))

    def test_set(self):
        a = Dict({"wagwaan": "popcaan", "flute": "don"})
        a["wagwaan"] = "hotskull"
        self.assertEquals(a["wagwaan"], "hotskull")

    def test_del(self):
        a = Dict({"wagwaan": "popcaan", "flute": "don"})
        del a["wagwaan"]
        self.assertRaises(KeyError, lambda: a["wagwaan"])
        def del_missing():
            del a["hotskull"]
        self.assertRaises(KeyError, del_missing)


if __name__ == "__main__":
    unittest.main()
