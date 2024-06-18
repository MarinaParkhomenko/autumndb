from enum import Enum

from autumn_db import DocumentId
from db_driver import DocumentOperation, CollectionOperation, CollectionName


class Event:

    def __init__(self, collection: CollectionName):
        self._collection = collection

    @property
    def event_code(self) -> int: ...

    @property
    def collection(self) -> CollectionName:
        return self._collection


class Subscriber:

    def callback(self, event: Event): ...


class CollectionOrientedEvent(Event):

    def __init__(self, collection: CollectionName, operation: CollectionOperation):
        super().__init__(collection)
        self._collection = collection
        self._operation = operation

    @property
    def event_code(self) -> int:
        return self._operation.value


class DocumentOrientedEvent(Event):

    def __init__(self, collection: CollectionName, operation: DocumentOperation, doc_id: DocumentId):
        super().__init__(collection)
        self._operation = operation
        self._doc_id = doc_id

    @property
    def event_code(self) -> int:
        return self._operation.value

    @property
    def document_id(self) -> DocumentId:
        return self._doc_id

    def __str__(self):
        return f"DocumentOrientedEvent, {self.document_id},{self.collection}"


class EventBus:

    def __init__(self):
        opers = list(CollectionOperation) + list(DocumentOperation)
        self._subscribers_by_oper = {oper.value: set() for oper in opers}

    def subscribe(self, oper: Enum, callback):
        if oper.value not in self._subscribers_by_oper.keys():
            raise Exception(f"Unknown oper {oper}")

        code = oper.value

        self._subscribers_by_oper[code] = set()
        self._subscribers_by_oper[code].add(callback)

    def unsubscribe(self, oper: Enum, callback):
        raise Exception('Is not implemented yet')

    def publish(self, oper: Enum, event: Event):
        code = oper.value
        if code not in self._subscribers_by_oper.keys():
            return

        for callback in self._subscribers_by_oper[code]:
            callback(event)
