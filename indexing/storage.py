from indexing.table import IndexTable
from datetime import datetime
from size import KB

from db_file.block_json import BlockJsonDbFile


class IndexedStorage:
    def __init__(self, dbf: BlockJsonDbFile, it: IndexTable):
        self.dbf = dbf
        self.it = it

    def get(self, rid):
        i = 0
        while True:
            try:
                indexes = self.it.fetch(i * self.it.block_size)
            except ValueError as e:
                break
            for key, address in indexes.items():
                if key == rid:
                    return self.dbf.fetch_json(address)
            i += 1

    def create(self, rid, updated_record):
        pass

    def scan(self, rid):
        i = 0
        while True:
            try:
                record = self.dbf.fetch_json(i * self.dbf.block_size)
            except ValueError:
                break
            if record['id'] == rid:
                return record
            i += 1

    def regenerate_indexes(self):
        self.it.raw_wipe()
        i, prev_idx_block = 0, 0
        while True:
            print(prev_idx_block, i)
            try:
                record = self.dbf.fetch_json(i * self.dbf.block_size)
                prev_idx_block = self.it.update(record['id'], i * self.dbf.block_size, prev_idx_block)
            except ValueError:
                break
            i += 1


dbf = BlockJsonDbFile('/home/pskd73/test.db', KB)
it = IndexTable('/home/pskd73/test.index', KB)
s = IndexedStorage(dbf, it)
# s.regenerate_indexes()
find_id = 'my_id_3000000'
#
t1 = datetime.now()
print(s.get(find_id))
print(datetime.now() - t1)

t2 = datetime.now()
print(s.scan(find_id))
print(datetime.now() - t2)

# dbf.init()
# for i in range(3000000):
#     dbf.write_json({
#         'id': 'my_id_{}'.format(i),
#         'name': 'Pramod',
#         'age': 29
#     })
