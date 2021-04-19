import os
from unittest import TestCase
from db_file.base import DbFile


class TestDbFile(TestCase):
    def test_write(self):
        f = DbFile('/tmp/test-db-file', block_size=30)
        f.init()
        pramod_bn = f.raw_write('pramod')
        self.assertEqual('pramod' + '\0'*24, f.raw_file_data())
        kumar_bn = f.raw_write('kumar')
        self.assertEqual('pramod' + '\0' * 24 + 'kumar' + '\0' * 25, f.raw_file_data())
        self.assertEqual('pramod', f.raw_fetch(pramod_bn))
        self.assertEqual('kumar', f.raw_fetch(kumar_bn))
        os.remove('/tmp/test-db-file')