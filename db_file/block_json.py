import json
from db_file.block import BlockDbFile


class BlockJsonDbFile(BlockDbFile):
    def write_json(self, data: dict, block_address: int = None) -> int:
        return self.write_block(json.dumps(data).encode(), block_address)

    def fetch_json(self, block_address: int) -> dict:
        return json.loads(self.fetch_block(block_address).decode())
