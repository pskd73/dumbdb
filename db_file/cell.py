from db_file.base import DbFile
from typing import List


class CellDbFile(DbFile):
    def __init__(self, fp: str, block_size: int, cell_sizes: List[int]):
        super(CellDbFile, self).__init__(fp)
        self.block_size = block_size
        self.cell_sizes = cell_sizes
        assert sum([s for s in self.cell_sizes if s != -1]) == self.block_size

    def get_cell_address(self, block_address: int, cell_idx: int):
        return block_address + sum(self.cell_sizes[:cell_idx])

    def write_to_cell(self, data: bytes, block_address: int, cell_idx: int):
        self.raw_write(data, self.cell_sizes[cell_idx], self.get_cell_address(block_address, cell_idx))

    def get_cell(self, block_address: int, cell_idx: int) -> bytes:
        return self.raw_fetch(self.get_cell_address(block_address, cell_idx), self.cell_sizes[cell_idx])
