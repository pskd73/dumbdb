import json
from db_file.block_json import BlockJsonDbFile


class IndexTable(BlockJsonDbFile):
    def _try_add_entry(self, existing_indexes: dict, start_key, end_key, address: int):
        existing_indexes['indexes'].append({
            'start_key': start_key,
            'end_key': end_key,
            'address': address
        })
        assert len(json.dumps(existing_indexes).encode()) <= self.block_size

    def update(self, start_key, end_key, value_address: int, address: int) -> int:
        try:
            existing_indexes = self.fetch_json(address)
        except ValueError:
            existing_indexes = {'indexes': []}
        try:
            self._try_add_entry(existing_indexes, start_key, end_key, value_address)
        except AssertionError:
            existing_indexes = {'indexes': []}
            self._try_add_entry(existing_indexes, start_key, end_key, value_address)
            address = None
        return self.write_json(existing_indexes, address)

    def fetch(self, address: int) -> dict:
        return self.fetch_json(address)
