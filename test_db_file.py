import os
from unittest import TestCase
from db_file import DbFile


class TestDbFile(TestCase):
    def test_write(self):
        f = DbFile('/tmp/test-db-file', block_size=30)
        f.init()
        f.write('pramod')
        self.assertEqual(f.raw_file_data(), 'pramod' + '\0'*24)
        f.write('kumar')
        self.assertEqual(f.raw_file_data(), 'pramod' + '\0' * 24 + 'kumar' + '\0' * 25)
        self.assertEqual(f.fetch(0), 'pramod')
        self.assertEqual(f.fetch(1), 'kumar')
        os.remove('/tmp/test-db-file')
