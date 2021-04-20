import json
from db_file.block_json import BlockJsonDbFile


class IndexTable(BlockJsonDbFile):
    def _can_add_new_index(self, existing_indexes: dict, key, address: int) -> bool:
        existing_indexes[key] = address
        return len(json.dumps(existing_indexes).encode()) <= self.block_size

    def update(self, key, value_address: int, address: int) -> int:
        try:
            existing_indexes = self.fetch_json(address)
        except ValueError:
            existing_indexes = {}
        if not self._can_add_new_index(existing_indexes, key, value_address):
            existing_indexes = {}
            address = None
            if key in existing_indexes:
                print('WARNING: Need to split the index block for key: {}, address: {}'.format(key, address))
        existing_indexes[key] = value_address
        return self.write_json(existing_indexes, address)

    def fetch(self, address: int) -> dict:
        return self.fetch_json(address)
