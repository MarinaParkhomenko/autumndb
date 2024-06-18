from autumn_db import DocumentId
from db_driver import DBDriver, CollectionName, Document

data = '{"firstname": "Valerii"}'

driver = DBDriver('127.0.0.1', 50001)

collection = CollectionName('test')
doc = Document(data)
doc_id = DocumentId('2024_02_07_08_32_20_594746')
print(doc_id)

doc_id = driver.create_document(collection, doc)

data = '{"firstname": "Maryna"}'
doc = Document(data)
driver.update_document(collection, doc_id, doc)

data = driver.read_document(collection, doc_id)
print(data)