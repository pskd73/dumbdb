import io
from db_file.padding import pad, unpad


class DbFile:
    def __init__(self, fp: str):
        self.file_path = fp

    def init(self):
        with open(self.file_path, 'w+') as f:
            f.write('')

    def raw_file_data(self) -> bytes:
        with open(self.file_path, 'rb') as f:
            return f.read()

    def raw_write(self, data: bytes, max_size: int, address: int = None) -> int:
        with open(self.file_path, 'rb+') as f:
            if address is not None:
                f.seek(address)
            else:
                f.seek(0, io.SEEK_END)
            starting_address = f.tell()
            f.write(pad(data, max_size))
            return starting_address

    def raw_fetch(self, block_address: int, size: int) -> bytes:
        with open(self.file_path, 'rb+') as f:
            f.seek(block_address)
            raw_data = f.read(size)
            if len(raw_data) == 0:
                raise ValueError('Invalid block address {}'.format(block_address))
            return unpad(raw_data)

    def raw_wipe(self):
        file = open(self.file_path, 'w')
        file.close()
