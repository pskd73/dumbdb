import json
from typing import List
from db_file.list import ListDbFile, to_str


class IndexTable(ListDbFile):
    def _try_add_entry(self, existing_indexes: List[List[str]], start_key, end_key, address: int):
        existing_indexes.append([start_key, end_key, str(address)])
        assert len(to_str(existing_indexes).encode()) <= self.block_size

    def update(self, start_key, end_key, value_address: int, address: int) -> int:
        try:
            existing_indexes = self.fetch_list(address)
        except ValueError:
            existing_indexes = []
        try:
            self._try_add_entry(existing_indexes, start_key, end_key, value_address)
        except AssertionError:
            existing_indexes = []
            self._try_add_entry(existing_indexes, start_key, end_key, value_address)
            address = None
        return self.write_list(existing_indexes, address)

    def fetch(self, address: int) -> List[List[str]]:
        return self.fetch_list(address)
