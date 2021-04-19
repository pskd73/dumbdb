import os
import base64
from db_file import DbFile
from typing import List, Tuple

from size import KB

TIndex = Tuple[str, int]
TIndexes = List[TIndex]


class IndexTable:
    def __init__(self, fp, block_size: int = KB):
        self.block_size = block_size
        self.file = DbFile(fp, self.block_size)
        self.file.init()

    @staticmethod
    def _encode_key(encoded_key: str) -> str:
        return base64.b64encode(encoded_key.encode()).decode()

    @staticmethod
    def _decode_key(key: str) -> str:
        return base64.b64decode(key.encode()).decode()

    @staticmethod
    def _to_raw_indexes(indexes: TIndexes) -> str:
        return '|'.join(['{},{}'.format(IndexTable._encode_key(index[0]), index[1]) for index in indexes])

    @staticmethod
    def _parse_raw_indexes(raw_indexes: str) -> TIndexes:
        indexes = []
        for raw_index in raw_indexes.split('|'):
            index = raw_index.split(',')
            indexes.append((IndexTable._decode_key(index[0]), int(index[1])))
        return indexes

    @staticmethod
    def _append_index(indexes: TIndexes, new_index: TIndex) -> TIndexes:
        for i in range(len(indexes)):
            if indexes[i][0] == new_index[0]:
                indexes[i] = (indexes[i][0], new_index[1])
                return indexes
        indexes.append(new_index)
        return indexes

    def _can_add_new_index(self, raw_existing_indexes: str, raw_new_index: str) -> bool:
        if len('{}|{}'.format(raw_existing_indexes, raw_new_index).encode()) <= self.block_size:
            return True
        return False

    def update(self, address: int, key: str, value_address: int):
        existing_indexes = self.file.fetch(address)
        if not self._can_add_new_index(existing_indexes, self._to_raw_indexes([(key, value_address)])):
            existing_indexes = ''
            address = None
        if existing_indexes == '':
            indexes = []
        else:
            indexes = self._parse_raw_indexes(existing_indexes)
        self._append_index(indexes, (key, value_address))
        return self.file.write(self._to_raw_indexes(indexes), address)

    def fetch(self, address: int) -> TIndexes:
        return self._parse_raw_indexes(self.file.fetch(address))

