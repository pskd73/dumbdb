import os
from unittest import TestCase
from db_file import DbFile


class TestDbFile(TestCase):
    def test_write(self):
        f = DbFile('/tmp/test-db-file', block_size=30)
        f.init()
        pramod_bn = f.write('pramod')
        self.assertEqual('pramod' + '\0'*24, f.raw_file_data())
        kumar_bn = f.write('kumar')
        self.assertEqual('pramod' + '\0' * 24 + 'kumar' + '\0' * 25, f.raw_file_data())
        self.assertEqual('pramod', f.fetch(pramod_bn))
        self.assertEqual('kumar', f.fetch(kumar_bn))
        os.remove('/tmp/test-db-file')
