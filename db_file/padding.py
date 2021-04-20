import sys


def pad(data: bytes, max_size: int) -> bytes:
    data_size = len(data)
    assert data_size <= max_size
    to_pad = max_size - data_size
    padded_data = data + b'\x00' * to_pad
    return padded_data


def unpad(data: bytes) -> bytes:
    for i, c in enumerate(data):
        if c.to_bytes(1, sys.byteorder) == b'\x00':
            return data[:i]
    return data
