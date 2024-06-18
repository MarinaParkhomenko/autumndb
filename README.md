# autumn_db
AutumnDB is distributed database and follows CP approach (CAP theorem)

Requirements:
- Python3.10

Usage examples:
- bring up an instance
1) create DB root holder
```commandline
mkdir test_holder
```
2) create Active Anti-Entropy configuration file
```
echo '' > aae_conf_1.json
```
The content of config file
```
{
  "current": {
    "snapshot_receiver": {
      "addr": "0.0.0.0",
      "port": 50002
    },
    "document_receiver": {
      "addr": "0.0.0.0",
      "port": 50003
    }
  },
  "neighbors": []
}
```
3) create python module
4) insert next code
```
os.environ['AAE_CONFIG_NAME'] = 'aae_conf_1'

holder_name = 'test_holder'
db_core = DBCoreEngine(holder_name)

endpoint = ClientEndpoint(50001, db_core)
endpoint.processing()
```
5) launch your module
6) use DB driver to execute CRUD operations (db_driver.py file)
```
from autumn_db import DocumentId
from db_driver import DBDriver, CollectionName, Document

data = '{"firstname": "Valerii"}'

driver = DBDriver('127.0.0.1', 50001)

collection = CollectionName('test')
doc = Document(data)
doc_id = DocumentId('2024_02_07_08_32_20_594746')
print(doc_id)

doc_id = driver.create_document(collection, doc)

data = '{"firstname": "Valerii2"}'
doc = Document(data)
driver.update_document(collection, doc_id, doc)

data = driver.read_document(collection, doc_id)
print(data)
```

You can bring up several instances on the same host

Use the same steps for the second instance but use other names, IP addrs and ports

If you want to use AAE mechanism then the AAE config file should be like
```
{
  "current": {
    "snapshot_receiver": {
      "addr": "0.0.0.0",
      "port": 50002
    },
    "document_receiver": {
      "addr": "0.0.0.0",
      "port": 50003
    }
  },
  "neighbors": [
    {
      "snapshot_receiver": {
      "addr": "0.0.0.0",
      "port": 50012
    },
    "document_receiver": {
      "addr": "0.0.0.0",
      "port": 50013
    }
    }
  ]
}
```
And second one like that
```
{
  "current": {
    "snapshot_receiver": {
      "addr": "0.0.0.0",
      "port": 50012
    },
    "document_receiver": {
      "addr": "0.0.0.0",
      "port": 50013
    }
  },
  "neighbors": [
    {
      "snapshot_receiver": {
      "addr": "0.0.0.0",
      "port": 50002
    },
    "document_receiver": {
      "addr": "0.0.0.0",
      "port": 50003
    }
    }
  ]
}
```

This database has the name Autumn because embedded active anti-entropy associates with distribution of yellow leaves in this period