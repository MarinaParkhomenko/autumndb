import json
import os
from enum import Enum
from queue import Queue

from autumn_db import DocumentId, DOC_ID_LENGTH
from autumn_db.data_storage.collection import CollectionOperations
from autumn_db.data_storage.collection.impl import CollectionOperationsImpl
from autumn_db.event_bus import EventBus, DocumentOrientedEvent
from db_driver import DocumentOperation, CollectionName


class DBOperationType(Enum):
    CREATE = 1
    UPDATE = 2
    READ = 3
    DELETE = 4


class DBOperation:

    def __init__(self, oper_type: DBOperationType, collection: str):
        self._oper_type = oper_type
        self._collection = collection
        self._is_finished = False

    @property
    def collection(self) -> str:
        return self._collection

    def finished(self):
        self._is_finished = True

    def is_finished(self) -> bool:
        return self._is_finished

    @property
    def operation_type(self) -> DBOperationType:
        return self._oper_type


class DocumentIdBasedOperation(DBOperation):

    def __init__(self, oper_type: DBOperationType, collection: str, document_id: DocumentId):
        super().__init__(oper_type, collection)
        self._document_id = document_id

    @property
    def document_id(self) -> DocumentId:
        return self._document_id


class ReadOperation(DocumentIdBasedOperation):

    def __init__(self, collection: str, document_id: DocumentId):
        super().__init__(DBOperationType.READ, collection, document_id)
        self._response = None

    @property
    def data(self) -> str:
        if self._response is None:
            raise Exception('Response is not ready')

        return self._response

    def set_data(self, data: str):
        if self._response is not None:
            raise Exception('Setting second time is forbidden')

        if data is None:
            data = str(data)

        self._response = data
        self.finished()


class DeleteOperation(DocumentIdBasedOperation):

    def __init__(self, collection: str, document_id: DocumentId):
        super().__init__(DBOperationType.DELETE, collection, document_id)


class CreateOperation(DBOperation):

    def __init__(self, collection: str, data: str):
        super().__init__(DBOperationType.CREATE, collection)
        self._data = data
        self._doc_id = DocumentId()

    @property
    def document_id(self) -> DocumentId:
        return self._doc_id

    @property
    def data(self) -> str:
        return self._data


class UpdateOperation(DocumentIdBasedOperation):

    def __init__(self, collection: str, document_id: DocumentId, data: str):
        super().__init__(DBOperationType.UPDATE, collection, document_id)
        self._data = data

    @property
    def data(self) -> str:
        return self._data


class DatabaseOperations:

    def _handle_create_operation(self, payload: bytearray): ...

    def _handle_update_operation(self, payload: bytearray): ...

    def _handle_delete_operation(self, payload: bytearray): ...

    def on_read(self, payload: bytearray): ...

    def create_collection(self, name: str): ...

    def delete_collection(self, name: str): ...


class DatabaseOperationsMockImpl(DatabaseOperations):

    def on_create(self, payload: bytearray):
        str = payload.decode('utf-8')
        _json = json.dumps(str)
        print(_json)

    def on_update(self, payload: bytearray):
        doc_id = payload[:DOC_ID_LENGTH:1]

        data = payload[DOC_ID_LENGTH::]
        str = data.decode('utf-8')
        _json = json.dumps(str)

        print(doc_id)
        print(_json)

    def on_delete(self, payload: bytearray):
        doc_id = payload[1:DOC_ID_LENGTH:1]
        print(doc_id)

    def on_read(self, payload: bytearray):
        doc_id = payload[:DOC_ID_LENGTH:1]
        print(doc_id)


class DBCoreEngine:

    def __init__(self, db_holder: str = None):
        if db_holder is None:
            db_holder = os.getcwd()

        self._db_holder = db_holder
        if not os.path.exists(self._db_holder):
            os.mkdir(self._db_holder)
        self._collections = self._discover_existing()

    def create_collection(self, name: str):
        if name in self._collections.keys():
            raise Exception(f"Collection {name} already exists")
        collection = CollectionOperationsImpl(name, self._db_holder)
        collection.create()

        self._collections[name] = collection

    def delete_collection(self, name: str):
        collection = self._collections[name]
        collection.delete()
        del self._collections[name]

    def _discover_existing(self) -> dict:
        collections_candidates = [f for f in os.scandir(self._db_holder) if f.is_dir()]

        exclude = set()
        for candidate in collections_candidates:
            dirs = [f.path for f in os.scandir(candidate) if f.is_dir()]

            for required_dir in ['data', 'metadata']:
                if required_dir in dirs:
                    exclude.add(candidate)
                    break

        collections = [entry for entry in collections_candidates if entry not in exclude]

        result = {collection.name: CollectionOperationsImpl(collection.name, self._db_holder) for collection in collections}
        return result

    @property
    def collections(self) -> dict:
        return self._collections

    def get_collection_safely(self, collection_name: str) -> CollectionOperations:
        if collection_name not in self._collections.keys():
            self.create_collection(collection_name)

        return self._collections[collection_name]


class DBOperationEngine:

    def __init__(self, db_core: DBCoreEngine):
        self._in_progress = set()
        self._read_queue = Queue()
        self._create_queue = Queue()
        self._update_queue = Queue()
        self._delete_queue = Queue()

        self._db_core_engine = db_core

        self._is_stopped = False

        self._event_bus = EventBus()

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def db_core(self) -> DBCoreEngine:
        return self._db_core_engine

    def add_operation(self, operation: DBOperation):
        if operation.operation_type == DBOperationType.CREATE:
            self._create_queue.put(operation)

        if operation.operation_type == DBOperationType.UPDATE:
            self._update_queue.put(operation)

        if operation.operation_type == DBOperationType.READ:
            self._read_queue.put(operation)

        if operation.operation_type == DBOperationType.DELETE:
            self._delete_queue.put(operation)

    def processing(self):
        while not self._is_stopped:
            deleted_per_iteration = set()
            if self._delete_queue.qsize() > 0:
                del_operation: DeleteOperation = self._delete_queue.get()

                self._handle_delete_operation(del_operation)
                deleted_per_iteration.add(del_operation.document_id)

            if self._read_queue.qsize() > 0:
                read_operation: ReadOperation = self._read_queue.get()
                if read_operation.document_id in deleted_per_iteration:
                    continue

                self._handle_read_operation(read_operation)

            if self._create_queue.qsize() > 0:
                create_operation: CreateOperation = self._create_queue.get()

                self._handle_create_operation(create_operation)

            if self._update_queue.qsize() > 0:
                update_operation: UpdateOperation = self._update_queue.get()
                if update_operation.document_id in deleted_per_iteration:
                    continue

                try:
                    self._handle_update_operation(update_operation)
                except Exception as e:
                    self._update_queue.put(update_operation)

    def _handle_create_operation(self, operation: CreateOperation):
        collection: CollectionOperations = self._db_core_engine.get_collection_safely(operation.collection)

        doc_id = str(operation.document_id)
        collection.create_document(doc_id, operation.data)

        ev = DocumentOrientedEvent(CollectionName(operation.collection), DocumentOperation.CREATE_DOC,
                                   DocumentId(doc_id))
        self.event_bus.publish(DocumentOperation.CREATE_DOC, ev)

    def _handle_update_operation(self, operation: UpdateOperation):
        collection: CollectionOperations = self._db_core_engine.get_collection_safely(operation.collection)

        filename = str(operation.document_id)

        collection.update_document(operation.document_id, operation.data)

        ev = DocumentOrientedEvent(CollectionName(operation.collection), DocumentOperation.UPDATE_DOC, DocumentId(filename))
        self.event_bus.publish(DocumentOperation.UPDATE_DOC, ev)

    def _handle_read_operation(self, operation: ReadOperation):
        collection: CollectionOperations = self._db_core_engine.get_collection_safely(operation.collection)

        try:
            data = collection.read_document(operation.document_id)
        except:
            data = None

        operation.set_data(data)

    def _handle_delete_operation(self, operation: DeleteOperation):
        collection: CollectionOperations = self._db_core_engine.get_collection_safely(operation.collection)

        filename = str(operation.document_id)
        collection.delete_document(filename)
