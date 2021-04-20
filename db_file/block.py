from db_file.base import DbFile


class BlockDbFile(DbFile):
    def __init__(self, fp: str, block_size: int):
        super(BlockDbFile, self).__init__(fp)
        self.block_size = block_size

    def write_block(self, data: bytes, block_address: int = None) -> int:
        return self.raw_write(data, self.block_size, block_address)

    def fetch_block(self, block_address: int) -> bytes:
        return self.raw_fetch(block_address, self.block_size)
