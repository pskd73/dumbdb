import os
from unittest import TestCase
from index_table import IndexTable


class TestIndexTable(TestCase):
    def test_base_condition(self):
        it = IndexTable('/tmp/test-index', 64)
        self.assertEqual(0, it.update('pramod', 873, 0))
        self.assertEqual(0, it.update('kumar', 890, 0))
        self.assertEqual(0, it.update('pramod', 890, 0))
        self.assertEqual(0, it.update('pramod1', 890, 0))
        self.assertEqual(0, it.update('pramod2', 890, 0))
        self.assertEqual(64, it.update('pramod3', 890, 0))
        os.remove('/tmp/test-index')
