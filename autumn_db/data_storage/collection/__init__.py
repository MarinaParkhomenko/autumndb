import datetime

from algorithms.ph2 import PH2
from algorithms.spectral_bloom_filter import SpectralBloomFilter
from autumn_db import DocumentId
from autumn_db.data_storage.data_access.impl import FilesystemAccess


file_access = FilesystemAccess()


class Operations(object):

    def __init__(self, pathname: str):
        self._pathname = pathname
        self._file_access = file_access


class DocumentOperations(Operations):

    def create(self, file: str): ...

    def update(self, file: str): ...

    def read(self) -> str: ...


class MetadataOperations(Operations):

    def set_updated_at(self, _datetime: datetime.datetime): ...

    def get_updated_at(self) -> datetime.datetime: ...

    def is_frozen(self) -> bool: ...

    def set_is_frozen(self, is_frozen: bool): ...


class CollectionOperations(object):

    def __init__(self, name: str, data_holder_path: str = None):
        import os
        if data_holder_path is None:
            data_holder_path = os.getcwd()

        self._data_holder_path = data_holder_path
        self._name = name
        self._full_path_to_collection = os.path.join(self._data_holder_path, self._name)

    @property
    def name(self) -> str:
        return self._name

    def create_document(self, filename: str, data: str, updated_at: datetime.datetime = None): ...

    def delete_document(self, filename: str): ...

    def document_exists(self, filename: str) -> bool: ...

    def create(self): ...

    def delete(self): ...

    def update_document(self, doc_id: DocumentId, data: str, updated_at: datetime.datetime = None): ...

    def get_updated_at(self, doc_id: DocumentId) -> datetime.datetime: ...

    def read_document(self, doc_id: DocumentId) -> str: ...

    def read_document_with_updated_at(self, doc_id: DocumentId) -> tuple: ...

    def set_updated_at(self, doc_id: DocumentId, updated_at: datetime.datetime): ...

    def doc_ids(self) -> set: ...

    def get_snapshot(self, doc_id: DocumentId) -> tuple: ...
