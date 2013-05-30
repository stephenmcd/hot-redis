#!/usr/bin/env python

import unittest

from hot_redis import List, Set


class ListTests(unittest.TestCase):

    def test_value(self):
        a = [4, 2, 0]
        self.assertEquals(List(a), a)

    def test_iter(self):
        a = [4, 2, 0]
        for i, x in enumerate(List(a)):
            self.assertEquals(x, a[i])

    def test_add(self):
        a = [4, 2, 0]
        b = [6, 6, 6]
        self.assertEquals(a + b, List(a) + List(b))
        self.assertEquals(a + b, List(a) + b)
        self.assertRaises(TypeError, lambda: List(a) + ["wulgus"])

    def test_iadd(self):
        a = [4, 2, 0]
        b = [6, 6, 6]
        c = List(a)
        d = List(b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)
        def iadd_other_type(l):
            l += ["wulgus"]
        self.assertRaises(TypeError, iadd_other_type, c)

    def test_mul(self):
        a = [4, 2, 0]
        i = 9000
        self.assertEquals(a * i, List(a) * i)

    def test_imul(self):
        a = [4, 2, 0]
        b = List(a)
        i = 9000
        b *= i
        self.assertEquals(a * i, b)

    def test_len(self):
        a = [4, 2, 0]
        self.assertEquals(len(a), len(List(a)))

    def test_get(self):
        a = [4, 2, 0] * 10
        b = List(a)
        self.assertEquals(a[4], b[4])
        self.assertEquals(a[3:12], b[3:12])
        self.assertEquals(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

    def test_set(self):
        a = [4, 2, 0]
        b = List(a)
        i = 9000
        a[1] = i
        self.assertNotEquals(a, b)
        b[1] = i
        self.assertEquals(a, b)
        def set_other_type(l):
            l[1] = "wulgus"
        self.assertRaises(TypeError, set_other_type, b)
        # todo: slice

    def test_del(self):
        a = [4, 2, 0]
        b = List(a)
        del a[1]
        self.assertNotEquals(a, b)
        del b[1]
        self.assertEquals(a, b)
        # todo: slice?

    def test_extend(self):
        a = [4, 2, 0]
        b = [6, 6, 6]
        c = List(a)
        a.extend(b)
        c.extend(b)
        self.assertEquals(a, c)
        self.assertRaises(TypeError, lambda: c.extend(["wulgus"]))

    def test_append(self):
        a = [4, 2, 0]
        b = List(a)
        i = 9000
        a.append(i)
        b.append(i)
        self.assertEquals(a, b)
        self.assertRaises(TypeError, lambda: b.append("wulgus"))

    def test_insert(self):
        a = [4, 2, 0]
        b = List(a)
        i = 9000
        a.insert(1, i)
        b.insert(1, i)
        self.assertEquals(a, b)
        self.assertRaises(TypeError, lambda: b.insert(1, "wulgus"))

    def test_pop(self):
        a = [4, 2, 0] * 10
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
        a = [4, 2, 0]
        b = List(a)
        a.reverse()
        b.reverse()
        self.assertEquals(a, b)

    def test_index(self):
        a = [4, 2, 0] * 10
        b = List(a)
        self.assertEquals(a.index(2), b.index(2))
        self.assertRaises(ValueError, lambda: b.index(9000))

    def test_count(self):
        a = [4, 2, 0] * 10
        b = List(a)
        self.assertEquals(a.count(2), b.count(2))
        self.assertEquals(a.count(9000), b.count(9000))

    def test_sort(self):
        a = [4, 2, 0] * 10
        b = List(a)
        a.sort()
        b.sort()
        self.assertEquals(a, b)
        a.sort(reverse=True)
        b.sort(reverse=True)
        self.assertEquals(a, b)


class SetTests(unittest.TestCase):

    def test_value(self):
        a = set([4, 2, 0])
        self.assertEquals(Set(a), a)

    def test_add(self):
        a = set([4, 2, 0])
        b = Set(a)
        i = 9000
        a.add(i)
        b.add(i)
        self.assertEquals(b, a)
        self.assertRaises(TypeError, lambda: b.add("wulgus"))

    def test_update(self):
        a = set([4, 2, 0])
        b = set([6, 6, 6])
        c = set([0, 6, 9])
        d = Set(a)
        a.update(b, c)
        d.update(b, c)
        self.assertEquals(d, a)
        self.assertRaises(TypeError, lambda: d.update(set(["wulgus"])))

    def test_pop(self):
        a = Set([4, 2, 0])
        i = len(a)
        b = a.pop()
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)

    def test_clear(self):
        a = Set([4, 2, 0])
        a.clear()
        self.assertEquals(len(a), 0)

    def test_remove(self):
        a = Set([4, 2, 0])
        i = len(a)
        b = 4
        a.remove(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(TypeError, lambda: a.remove("wulgus"))
        self.assertRaises(KeyError, lambda: a.remove(9000))

    def test_discard(self):
        a = Set([4, 2, 0])
        i = len(a)
        b = 4
        a.discard(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(TypeError, lambda: a.discard("wulgus"))
        self.assertEquals(a.discard(9000), None)

    def test_len(self):
        a = set([4, 2, 0])
        b = Set(a)
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = Set([4, 2, 0])
        self.assertIn(4, a)
        self.assertNotIn(9000, a)

    def test_and(self):
        a = set([4, 2, 0])
        b = set([4, 2, 1])
        self.assertEquals(a & b, Set(a) & b)

    def test_iand(self):
        a = set([4, 2, 0])
        b = set([4, 2, 1])
        c = Set(a)
        a &= b
        c &= b
        self.assertEquals(c, a)

    def test_rand(self):
        a = set([4, 2, 0])
        b = set([4, 2, 1])
        self.assertEquals(b & a, b & Set(a))
        self.assertEquals(b & a, Set(b) & Set(a))

    def test_intersection(self):
        a = set([4, 2, 0])
        b = set([4, 2, 1])
        c = set([4, 2, 2])
        d = Set(a)
        self.assertEquals(a.intersection(b), d.intersection(b))
        self.assertEquals(a.intersection(b, c), d.intersection(b, c))
        self.assertEquals(a.intersection(b, c), d.intersection(Set(b), c))
        self.assertEquals(a.intersection(b, c), d.intersection(b, Set(c)))
        self.assertEquals(a.intersection(b, c), d.intersection(Set(b), Set(c)))

if __name__ == "__main__":
    unittest.main()
