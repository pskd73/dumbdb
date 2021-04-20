import os
from unittest import TestCase
from db_file.base import DbFile
from db_file.cell import CellDbFile


class TestCell(TestCase):
    def test_write_to_cell(self):
        f = DbFile('/tmp/test-cell')
        f.init()
        cf = CellDbFile(f, 30, [5, 5, 20])
        self.assertRaises(AssertionError, lambda: cf.write_to_cell(b'abcdef', 0, 1))
        cf.write_to_cell(b'abc', 0, 0)
        self.assertEqual(b'abc\x00\x00', f.raw_file_data())
        cf.write_to_cell(b'abcd', 0, 1)
        self.assertEqual(b'abc\x00\x00abcd\x00', f.raw_file_data())
        cf.write_to_cell(b'abcx', 0, 1)
        self.assertEqual(b'abc\x00\x00abcx\x00', f.raw_file_data())
        cf.write_to_cell(b'pramodkumar', 0, 2)
        self.assertEqual(b'abc\x00\x00abcx\x00pramodkumar' + b'\x00'*9, f.raw_file_data())
        os.remove('/tmp/test-cell')

    def test_get_cell(self):
        f = DbFile('/tmp/test-cell')
        f.init()
        cf = CellDbFile(f, 30, [5, 5, 20])
        cf.write_to_cell(b'abc', 0, 0)
        cf.write_to_cell(b'abcd', 0, 1)
        cf.write_to_cell(b'abcx', 0, 1)
        cf.write_to_cell(b'pramodkumar', 0, 2)
        self.assertEqual(b'abc', cf.get_cell(0, 0))
        self.assertEqual(b'abcx', cf.get_cell(0, 1))
        self.assertEqual(b'pramodkumar', cf.get_cell(0, 2))
