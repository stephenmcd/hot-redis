
import unittest

from hot_redis import List


class ListTests(unittest.TestCase):

    def test_value(self):
        l = [1, 2, 3]
        self.assertEquals(List(l).value, l)

    def test_iter(self):
        l = [1, 2, 3]
        for i, x in enumerate(List(l)):
            self.assertEquals(x, l[i])

    def test_add(self):
        l1 = [1, 2, 3]
        l2 = [4, 5, 6]
        self.assertEquals(l1 + l2, List(l1) + List(l2))
        self.assertEquals(l1 + l2, List(l1) + l2)

    def test_iadd(self):
        l1 = [1, 2, 3]
        l2 = [4, 5, 6]
        l3 = List(l1)
        l4 = List(l2)
        l4 += l3
        l3 += l2
        self.assertEquals(l1 + l2, l3)
        self.assertEquals(l2 + l1, l4)

if __name__ == '__main__':
    unittest.main()
