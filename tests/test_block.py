from unittest import TestCase

from db_file.block import BlockDbFile


class TestBlockDbFile(TestCase):
    def test_write_block(self):
        bf = BlockDbFile('/tmp/test-block', 30)
        bf.init()
        addr = bf.write_block(b'pramod')
        self.assertEqual(0, addr)
        self.assertEqual(b'pramod'+b'\x00'*24, bf.raw_file_data())
        self.assertRaises(AssertionError, lambda: bf.write_block(b'pramod' * 6))
        addr = bf.write_block(b'kumar')
        self.assertEqual(30, addr)
        self.assertEqual(b'pramod' + b'\x00'*24+b'kumar'+b'\x00'*25, bf.raw_file_data())

    def test_fetch_block(self):
        bf = BlockDbFile('/tmp/test-block', 30)
        bf.init()
        bf.write_block(b'pramod')
        self.assertEqual(b'pramod', bf.fetch_block(0))
        bf.write_block(b'kumar')
        self.assertEqual(b'kumar', bf.fetch_block(30))
