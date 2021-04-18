import os
import logging
from storage import Storage


class PhysicalStorage(Storage):
    def __init__(self, db_file_path):
        self.DB_FILE_PATH = db_file_path

    def __get_key_path(self, key):
        return os.path.join(self.DB_FILE_PATH, key)

    def put(self, key, value):
        logging.info('putting {} into physical storage'.format(key))
        with open(self.__get_key_path(key), 'w+') as f:
            f.write(str(value) + '\n')

    def get(self, key):
        logging.info('getting {} from physical storage'.format(key))
        key_path = self.__get_key_path(key)
        if not os.path.exists(key_path):
            return None
        with open(key_path, 'r') as f:
            return f.read()
