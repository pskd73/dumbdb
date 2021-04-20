import json
from db_file.base import DbFile


class JsonDbFile(DbFile):
    def write(self, data: dict, block_size: int, block_address: int = None) -> int:
        return self.raw_write(json.dumps(data).encode(), block_size, block_address)

    def fetch(self, block_address: int, size: int) -> dict:
        return json.loads(self.raw_fetch(block_address, size).decode())
