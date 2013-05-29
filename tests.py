
import unittest

from hot_redis import List


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


    def test_count(self):
        pass
    def test_sort(self):
        pass


if __name__ == "__main__":
    unittest.main()
