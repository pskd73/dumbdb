import os
from unittest import TestCase
from index_table import IndexTable


class TestIndexTable(TestCase):
    def test_base_condition(self):
        it = IndexTable('/tmp/test-index', 64)
        self.assertEqual(0, it.update(0, 'pramod', 873))
        self.assertEqual(0, it.update(0, 'kumar', 890))
        self.assertEqual(0, it.update(0, 'pramod', 890))
        self.assertEqual(0, it.update(0, 'pramod1', 890))
        self.assertEqual(0, it.update(0, 'pramod2', 890))
        self.assertEqual(64, it.update(0, 'pramod3', 890))
        os.remove('/tmp/test-index')
