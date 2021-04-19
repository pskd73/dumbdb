import io

from size import KB


class DbFile:
    def __init__(self, file_path: str, block_size: int = 8*KB):
        self.block_size = block_size
        self.file_path = file_path

    def _pad(self, data: str):
        data_size = len(data.encode())
        assert data_size <= self.block_size
        to_pad = self.block_size - data_size
        padded_data = data + '\0' * to_pad
        return padded_data

    def _unpad(self, data: str):
        for i, c in enumerate(data):
            if c == '\0':
                return data[:i]
        return data

    def init(self):
        with open(self.file_path, 'w+') as f:
            f.write('')

    def raw_file_data(self):
        with open(self.file_path, 'r') as f:
            return f.read()

    def raw_write(self, data: str, block_address: int = None) -> int:
        with open(self.file_path, 'r+') as f:
            if block_address is not None:
                f.seek(block_address)
            else:
                f.seek(0, io.SEEK_END)
            starting_address = f.tell()
            f.write(self._pad(data))
            return starting_address

    def raw_fetch(self, block_address: int) -> str:
        with open(self.file_path, 'r+') as f:
            f.seek(block_address)
            raw_data = f.read(self.block_size)
            if raw_data == '':
                raise ValueError('Invalid block address {}'.format(block_address))
            return self._unpad(raw_data)

    def raw_wipe(self):
        file = open(self.file_path, 'w')
        file.close()
