import base64
from typing import Tuple, List
from db_file.block import BlockDbFile


def from_str(s: str) -> List[List[str]]:
    raw_list = s.split('|')
    pairs = []
    for raw_sl in raw_list:
        pairs.append([base64.b64decode(v).decode() for v in raw_sl.split(',')])
    return pairs


def to_str(pairs: List[List[str]]) -> str:
    return '|'.join([','.join([base64.b64encode(e.encode()).decode() for e in sl]) for sl in pairs])


class ListDbFile(BlockDbFile):
    def write_json(self, data: List[List[str]], block_address: int = None) -> int:
        return self.write_block(to_str(data).encode(), block_address)

    def fetch_json(self, block_address: int) -> List[List[str]]:
        return from_str(self.fetch_block(block_address).decode())
