import os
import logging
from storage import Storage

KB = 1024


class PhysicalStorage(Storage):
    def __init__(self, db_file_path):
        self.BLOCK_SIZE = 30
        self.DB_FILE_PATH = db_file_path
        self.next_vacant_block = 0

    def __get_key_path(self, key):
        return os.path.join(self.DB_FILE_PATH, key)

    def put(self, key, value):
        logging.info('putting {} into physical storage'.format(key))
        with open(self.__get_key_path(key), 'w+') as f:
            f.write(str(value) + '\n')

    def get(self, key):
        logging.info('getting {} from physical storage'.format(key))
        key_path = self.__get_key_path(key)
        if not os.path.exists(key_path):
            return None
        with open(key_path, 'r') as f:
            return f.read()

    def pad(self, data: str):
        data_size = len(data.encode())
        assert data_size <= self.BLOCK_SIZE
        to_pad = self.BLOCK_SIZE - data_size
        padded_data = data + '\0' * to_pad
        return padded_data

    def unpad(self, data: str):
        for i, c in enumerate(data):
            if c == '\0':
                return data[:i]
        return data

    def raw_file_data(self):
        with open(self.DB_FILE_PATH, 'r') as f:
            return f.read()

    def write(self, data: str) -> int:
        start_pos = self.next_vacant_block
        with open(self.DB_FILE_PATH, 'w') as f:
            print('seeking to', start_pos)
            f.seek(start_pos)
            f.write(self.pad(data))
            self.next_vacant_block += self.BLOCK_SIZE
            return start_pos

    def fetch(self, block_number: int) -> str:
        with open(self.DB_FILE_PATH, 'r') as f:
            f.seek(block_number)
            raw_data = f.read(self.BLOCK_SIZE)
            return self.unpad(raw_data)


ps = PhysicalStorage('db')
print(ps.write('pramod'))
print(ps.write('kumar'))
print(ps.write('pskd'))
print(ps.raw_file_data())
