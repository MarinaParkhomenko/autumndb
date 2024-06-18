import datetime
import json
import os
import shutil
import threading

from algorithms import to_bytearray_from_values
from algorithms.ph2 import PH2
from algorithms.spectral_bloom_filter import SpectralBloomFilter
from autumn_db.autumn_db import DocumentId
from autumn_db.data_storage.collection import DocumentOperations, MetadataOperations, CollectionOperations, file_access


def calculate_sbf(_bytearray: bytearray) -> SpectralBloomFilter:
    res = SpectralBloomFilter()
    res.add(_bytearray)

    return res


def calculate_ph2(_bytearray: bytearray):
    res = PH2()
    res.append(_bytearray)

    return res


class MetadataOperationsImpl(MetadataOperations):
    UPDATED_AT_KEY = 'updated_at'
    IS_FROZEN_KEY = 'is_frozen'

    def set_updated_at(self, _datetime: datetime.datetime):
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)

        metadata[MetadataOperationsImpl.UPDATED_AT_KEY] = _datetime.strftime(DocumentId.UTC_FORMAT)
        content = json.dumps(metadata)

        self._file_access.update(self._pathname, content)

    def get_updated_at(self) -> datetime.datetime:
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)
        updated_at_str = metadata[MetadataOperationsImpl.UPDATED_AT_KEY]

        res = datetime.datetime.strptime(updated_at_str, DocumentId.UTC_FORMAT)
        return res

    def is_frozen(self) -> bool:
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)

        res = metadata[MetadataOperationsImpl.IS_FROZEN_KEY]
        return res

    def set_is_frozen(self, is_frozen: bool):
        content = self._file_access.read(self._pathname)
        metadata = json.loads(content)

        metadata[MetadataOperationsImpl.IS_FROZEN_KEY] = is_frozen
        content = json.dumps(metadata)

        self._file_access.update(self._pathname, content)


class DocumentOperationsImpl(DocumentOperations):

    def create(self, file: str):
        self._file_access.create(self._pathname, file)

    def update(self, file: str):
        self._file_access.update(self._pathname, file)

    def read(self) -> str:
        res = self._file_access.read(self._pathname)

        return res


class CollectionOperationsImpl(CollectionOperations):

    def __init__(self, name: str, data_holder_path: str = None):
        super().__init__(name, data_holder_path)
        self._lock = threading.Lock()

        # self._doc_ids = set()
        self._doc_snapshot_mapping = dict()
        self._init_initial_doc_ids()

    def _init_initial_doc_ids(self):
        for dirpath, _, filenames in os.walk(os.path.join(self._full_path_to_collection, 'data')):
            doc_ids = set(filenames)
            with self._lock:
                if self.name not in self._doc_snapshot_mapping.keys():
                    self._doc_snapshot_mapping[self.name] = set()

                self._doc_snapshot_mapping[self.name].union(doc_ids)

    def __len__(self):
        return len(self._doc_snapshot_mapping.keys())

    def create(self):
        path_to_data = os.path.join(self._full_path_to_collection, 'data')
        path_to_metadata = os.path.join(self._full_path_to_collection, 'metadata')
        with self._lock:
            os.makedirs(path_to_data)
            os.makedirs(path_to_metadata)

    def delete(self):
        shutil.rmtree(self._full_path_to_collection)

    def create_document(self, filename: str, data: str, updated_at: datetime.datetime = None):
        if updated_at is None:
            updated_at = datetime.datetime.utcnow()

        data_pathname = os.path.join(self._full_path_to_collection, 'data', filename)
        metadata_pathname = os.path.join(self._full_path_to_collection, 'metadata', filename)

        file_access.create(data_pathname, data)

        metadata_content = {
            MetadataOperationsImpl.UPDATED_AT_KEY: updated_at.strftime(
                    DocumentId.UTC_FORMAT),
            MetadataOperationsImpl.IS_FROZEN_KEY: False
        }
        metadata_content_str = json.dumps(metadata_content)
        file_access.create(metadata_pathname, metadata_content_str)

        #calc Snapshot
        _bytearray = to_bytearray_from_values(json.loads(data))
        sbf = calculate_sbf(_bytearray)
        ph2 = calculate_ph2(_bytearray)

        with self._lock:
            self._doc_snapshot_mapping[filename] = (sbf, ph2)

    def delete_document(self, filename: str):
        data_pathname = os.path.join(self._full_path_to_collection, 'data', filename)
        metadata_pathname = os.path.join(self._full_path_to_collection, 'metadata', filename)

        file_access.delete(data_pathname)
        file_access.delete(metadata_pathname)

    def document_exists(self, filename: str) -> bool:
        path = os.path.join(self._full_path_to_collection, 'data', filename)
        return os.path.isfile(path)

    def _get_document_operator(self, filename: str) -> DocumentOperations:
        pathname = os.path.join(self._full_path_to_collection, 'data', filename)

        res = DocumentOperationsImpl(pathname)
        return res

    def _get_metadata_operator(self, filename: str) -> MetadataOperations:
        pathname = os.path.join(self._full_path_to_collection, 'metadata', filename)

        res = MetadataOperationsImpl(pathname)
        return res

    def update_document(self, doc_id: DocumentId, data: str, updated_at: datetime.datetime = None):
        if updated_at is None:
            updated_at = datetime.datetime.utcnow()

        doc_id = str(doc_id)
        doc_oper = self._get_document_operator(doc_id)
        metadata_oper = self._get_metadata_operator(doc_id)

        # calc Snapshot
        _bytearray = to_bytearray_from_values(json.loads(data))
        sbf = calculate_sbf(_bytearray)
        ph2 = calculate_ph2(_bytearray)

        with self._lock:
            doc_oper.update(data)
            metadata_oper.set_updated_at(updated_at)
            self._doc_snapshot_mapping[doc_id] = (sbf, ph2)

    def get_updated_at(self, doc_id: DocumentId) -> datetime.datetime:
        doc_id = str(doc_id)
        metadata_oper = self._get_metadata_operator(doc_id)

        with self._lock:
            updated_at = metadata_oper.get_updated_at()

        return updated_at

    def set_updated_at(self, doc_id: DocumentId, updated_at: datetime.datetime):
        doc_id = str(doc_id)
        metadata_oper = self._get_metadata_operator(doc_id)

        with self._lock:
            metadata_oper.set_updated_at(updated_at)

    def read_document(self, doc_id: DocumentId) -> str:
        doc_id = str(doc_id)
        doc_oper = self._get_document_operator(doc_id)

        with self._lock:
            data = doc_oper.read()

        return data

    def read_document_with_updated_at(self, doc_id: DocumentId) -> tuple:
        doc_id = str(doc_id)
        doc_oper = self._get_document_operator(doc_id)
        metadata_oper = self._get_metadata_operator(doc_id)

        with self._lock:
            data = doc_oper.read()
            updated_at = metadata_oper.get_updated_at()

        return data, updated_at

    def doc_ids(self) -> set:
        with self._lock:
            ids = self._doc_snapshot_mapping.keys()

        res = set(ids)
        return res

    def get_snapshot(self, doc_id: DocumentId) -> tuple:
        _doc_id = str(doc_id)
        if _doc_id not in self._doc_snapshot_mapping.keys():
            return None

        res = self._doc_snapshot_mapping[_doc_id]
        return res
