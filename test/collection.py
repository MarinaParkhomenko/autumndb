import json
import unit_test

# TODO we need this import to avoid circular import
from autumn_db.autumn_db import DocumentId

from autumn_db.data_storage.collection import CollectionOperations
from autumn_db.data_storage.collection.impl import CollectionOperationsImpl

collection_name = 'users'
filename = 'test1'

user_data = {
    'firstname': 'Valerii',
    'lastname': 'Nikitin'
}
data_str = json.dumps(user_data)


class TestCollection(unittest.TestCase):

    def setUp(self) -> None:
        self._collection: CollectionOperations = CollectionOperationsImpl(collection_name)

        self._collection.create()
        self._collection.create_document(filename, data_str)

    def tearDown(self) -> None:
        self._collection.delete()

    def test_read_success(self):
        data_operator = self._collection.get_document_operator(filename)

        read_data = data_operator.read()
        self.assertEqual(read_data, data_str)

    def test_update_data_success(self):
        data_operator = self._collection.get_document_operator(filename)

        new_data = {
            'firstname': 'Marine',
            'lastname': 'Miller'
        }
        new_data_str = json.dumps(new_data)
        data_operator.update(new_data_str)

        read_data = data_operator.read()

        self.assertEqual(read_data, new_data_str)

    #def test_delete_document_success(self):
        # TODO delete document and check files are absent under data & metadata dirs

    #def test_metadata_updated_success(self):
        # TODO get the value of 'updated_at', do update of file and take the 'updated_at' again; check that value AFTER is later

    #def test_metadata_is_frozen_success(self):
        # TODO change value of 'is_frozen' and check that value has been changed
