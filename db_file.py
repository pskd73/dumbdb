from typing import Union

KB = 1024


class DbFile:
    def __init__(self, file_path: str, block_size: int = 8*KB):
        self.block_size = block_size
        self.file_path = file_path
        self.next_vacant_block = 0

    def _pad(self, data: str):
        data_size = len(data.encode())
        assert data_size <= self.block_size
        to_pad = self.block_size - data_size
        padded_data = data + '\0' * to_pad
        return padded_data

    def _unpad(self, data: str):
        for i, c in enumerate(data):
            if c == '\0':
                return data[:i]
        return data

    def _block_number_to_offset(self, block_number: int):
        return block_number * self.block_size

    def init(self):
        with open(self.file_path, 'w+') as f:
            f.write('')

    def raw_file_data(self):
        with open(self.file_path, 'r') as f:
            return f.read()

    def write(self, data: str) -> int:
        start_pos = self._block_number_to_offset(self.next_vacant_block)
        with open(self.file_path, 'r+') as f:
            f.seek(start_pos)
            f.write(self._pad(data))
            self.next_vacant_block += 1
            return start_pos

    def fetch(self, block_number: int) -> str:
        with open(self.file_path, 'r+') as f:
            f.seek(self._block_number_to_offset(block_number))
            raw_data = f.read(self.block_size)
            return self._unpad(raw_data)
