from unittest import TestCase
from db_file.padding import pad, unpad


class TestPadding(TestCase):
    def test_pad(self):
        padded = pad('pramod'.encode(), 10)
        self.assertEqual(10, len(padded))
        self.assertEqual(b'pramod' + b'\x00' * 4, padded)
        self.assertEqual(b'\x00' * 10, pad(''.encode(), 10))
        self.assertRaises(AssertionError, lambda: pad(b'\x00' * 20, 10))

    def test_unpad(self):
        self.assertEqual(b'pramod', unpad(b'pramod' + b'\x00' * 20))
        self.assertEqual(b'', unpad(b'\x00' * 20 + b'pramod'))
