import json

from autumn_db.data_storage.collection.impl import CollectionOperationsImpl

users = CollectionOperationsImpl('users')

users.create()

user_data = {
    'firstname': 'Valerii',
    'lastname': 'Nikitin'
}
data_str = json.dumps(user_data)

filename = 'test1'
data_operator = users.get_document_operator(filename)
metadata = users.get_metadata_operator(filename)

users.create_document(filename, data_str)

print(data_operator.read())
print(metadata.get_updated_at())

users.delete()
