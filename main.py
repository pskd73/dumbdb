from cache import Cache
from physical_storage import PhysicalStorage
from datetime import datetime

DB_FILE_PATH = '/tmp/dumbdb'

ps = PhysicalStorage(DB_FILE_PATH)
cps = Cache(5, ps)
cps.put('name', 'pramodkumar')
t1 = datetime.now()
for i in range(1000000):
    cps.get('name_{}'.format(i))
t2 = datetime.now()
print(t2 - t1)
