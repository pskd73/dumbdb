import os
from unittest import TestCase
from db_file.base import DbFile


class TestDbFile(TestCase):
    def test_write(self):
        f = DbFile('/tmp/test-db-file')
        f.init()
        pramod_bn = f.raw_write(b'pramod', 30)
        self.assertEqual(b'pramod' + b'\x00'*24, f.raw_file_data())
        kumar_bn = f.raw_write(b'kumar', 30)
        self.assertEqual(b'pramod' + b'\x00' * 24 + b'kumar' + b'\x00' * 25, f.raw_file_data())
        self.assertEqual(b'pramod', f.raw_fetch(pramod_bn, 30))
        self.assertEqual(b'kumar', f.raw_fetch(kumar_bn, 30))
        os.remove('/tmp/test-db-file')
