import os
from unittest import TestCase

from db_file.base import DbFile
from index_table import IndexTable


class TestIndexTable(TestCase):
    def test_base_condition(self):
        f = DbFile('/tmp/test-index', 64)
        f.init()
        it = IndexTable(f)
        self.assertEqual(0, it.update('pramod', 873, 0))
        self.assertEqual(0, it.update('kumar', 890, 0))
        self.assertEqual(0, it.update('pramod', 890, 0))
        self.assertEqual(0, it.update('pramod1', 890, 0))
        self.assertEqual(0, it.update('pramod2', 890, 0))
        self.assertEqual(64, it.update('pramod3', 890, 0))
        os.remove('/tmp/test-index')
