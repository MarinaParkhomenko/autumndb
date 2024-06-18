import json
import os
import shutil
import threading
import unittest
from time import sleep

from autumn_db.autumn_db import DBCoreEngine, DBOperationEngine, CreateOperation, ReadOperation


user_data = {
    'firstname': 'Valerii',
    'lastname': 'Nikitin'
}
data_str = json.dumps(user_data)

holder_path = 'test_folder'
collection_name = 'users'
try:
    shutil.rmtree(holder_path)
except:
    pass
os.mkdir(holder_path)

db_core = DBCoreEngine(holder_path)
db_core.create_collection(collection_name)
db_operation = DBOperationEngine(db_core)


class TestCollection(unittest.TestCase):

    def setUp(self) -> None:
        th = threading.Thread(target=db_operation.processing, args=())
        th.start()

    def tearDown(self) -> None:
        db_operation._is_stopped = True
        db_core.delete_collection(collection_name)

    def test_create_read_success(self):
        create = CreateOperation(collection_name, data_str)
        db_operation.add_operation(create)

        # we give some delay before read operation
        sleep(1)

        read = ReadOperation(collection_name, create.document_id)
        db_operation.add_operation(read)

        while not read.is_finished():
            sleep(1)

        read_data = read.data
        self.assertEqual(read_data, data_str)

