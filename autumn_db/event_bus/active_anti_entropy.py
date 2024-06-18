import json
import logging
import socket
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from queue import Queue

from typing import List

from algorithms.ph2 import PH2
from algorithms.spectral_bloom_filter import SpectralBloomFilter
from autumn_db import DocumentId
from autumn_db.autumn_db import DBCoreEngine, DBOperationEngine
from autumn_db.data_storage.collection import CollectionOperations
from autumn_db.event_bus import Event, Subscriber, DocumentOrientedEvent
from db_driver import CollectionName, Document, DRIVER_COLLECTION_NAME_LENGTH_BYTES, DRIVER_BYTEORDER, \
    DRIVER_DOCUMENT_ID_LENGTH, CollectionOperation, DocumentOperation, send_message_to


_timeout = 0.2

@dataclass
class Endpoint:
    addr: str
    port: int


@dataclass
class NodeConfig:
    snapshot_receiver: Endpoint
    document_receiver: Endpoint

    def __post_init__(self):
        self.snapshot_receiver = Endpoint(**self.snapshot_receiver)
        self.document_receiver = Endpoint(**self.document_receiver)


@dataclass
class AAEConfig:
    current: NodeConfig
    neighbors: List[NodeConfig]

    def __post_init__(self):
        self.current = NodeConfig(**self.current)
        self.neighbors = [NodeConfig(**entry) for entry in self.neighbors]


class DocumentReceiver:
    BUFFER_SIZE = 1

    def __init__(self, port: int):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._port = port
        self._socket.bind(
            ('0.0.0.0', port)
        )
        self._socket.settimeout(_timeout)
        self._socket.listen()

    def get_document_and_metadata(self) -> bytearray:
        try:
            connection, client_address = self._socket.accept()
        except socket.timeout:
            return None

        data = bytearray()
        while True:
            part = connection.recv(DocumentReceiver.BUFFER_SIZE)
            if not part:
                break

            data.extend(part)

        return data


class AAEOperationType(Enum):
    TERMINATE_SESSION: int = 0
    SENDING_SNAPSHOT: int = 1
    SENDING_TIMESTAMP: int = 2

    @staticmethod
    def get_by_value(value: int):
        for t in AAEOperationType:
            if t.value == value:
                return t

        raise Exception(f"Could not map to the type value {value}")


class AAESnapshot:

    def get(self) -> bytearray: ...


class AAECommunication(AAESnapshot):

    def __init__(self, _type: AAEOperationType):
        self._type = _type

    def get_opcode(self) -> bytes:
        res = self._type.value.to_bytes(1, byteorder=DRIVER_BYTEORDER)
        return res

    def get(self) -> bytearray: ...


class Snapshot:

    def __init__(self, sbf: SpectralBloomFilter, hash: PH2):
        b_sbf = sbf.get()
        b_hash = hash.hashing()

        self._bytearray = bytearray()
        parts = [
            b_sbf,
            b_hash,
        ]
        for part in parts:
            self._bytearray.extend(part)

    def get(self) -> bytearray:
        return self._bytearray

    def __str__(self):
        return str(self._bytearray)

    def __repr__(self):
        return self.__str__()


class AAECheckSnapshot(AAECommunication):

    def __init__(self, collection_name: str, doc_id: str, snapshot: Snapshot):
        super().__init__(AAEOperationType.SENDING_SNAPSHOT)
        b_collection_name = collection_name.encode('utf-8')
        collection_name_len = len(b_collection_name)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)
        b_doc_id = doc_id.encode()

        self._bytearray = bytearray()
        parts = [
            self.get_opcode(),
            collection_name_len_encoded,
            b_collection_name,
            b_doc_id,
            snapshot.get(),
        ]
        for part in parts:
            self._bytearray.extend(part)

    def get(self) -> bytearray:
        return self._bytearray


class AAERequestSnapshot(AAECommunication):

    def __init__(self, collection_name: str, doc_id: str):
        super().__init__(AAEOperationType.SENDING_TIMESTAMP)
        b_collection_name = collection_name.encode('utf-8')
        collection_name_len = len(b_collection_name)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)

        b_doc_id = doc_id.encode()

        self._bytearray = bytearray()
        parts = [
            self.get_opcode(),
            collection_name_len_encoded,
            b_collection_name,
            b_doc_id
        ]
        for part in parts:
            self._bytearray.extend(part)

    def get(self) -> bytearray:
        return self._bytearray


class AAEAnswererWorker:
    BUFFER_SIZE = 46
    SENDING_TIMESTAMP_PAYLOAD_PART = bytes([AAEOperationType.SENDING_TIMESTAMP.value])
    TERMINATION_PAYLOAD = bytes([AAEOperationType.TERMINATE_SESSION.value])

    def __init__(self, addr: str, port: int, db_core: DBCoreEngine, receivers: List[NodeConfig]):
        super().__init__()
        self._socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self._socket.settimeout(_timeout)
        self._socket.bind(
            (
                addr, port
            )
        )
        self._listening_port = port

        self._db_core = db_core
        self._receivers = receivers

    def processing(self):
        try:
            payload, addr_port = self._socket.recvfrom(AAEAnswererWorker.BUFFER_SIZE)
        except socket.timeout:
            return None

        oper_code = payload[0]
        payload = payload[1::1]
        operation_type = AAEOperationType.get_by_value(oper_code)

        def send_timestamp(timestamp: datetime):
            s_timestamp = datetime.strftime(timestamp, DocumentId.UTC_FORMAT)
            b_timestamp = s_timestamp.encode('utf-8')

            _bytearray = bytearray()
            parts = [
                AAEAnswererWorker.SENDING_TIMESTAMP_PAYLOAD_PART,
                b_timestamp,
            ]
            for part in parts:
                _bytearray.extend(part)

            self._socket.sendto(_bytearray, addr_port)

        if operation_type == AAEOperationType.SENDING_SNAPSHOT:
            collection_name_length_bytes = payload[:DRIVER_COLLECTION_NAME_LENGTH_BYTES:1]
            payload = payload[DRIVER_COLLECTION_NAME_LENGTH_BYTES::]

            collection_name_length = int.from_bytes(collection_name_length_bytes, DRIVER_BYTEORDER, signed=False)
            collection_name_bytes = payload[:collection_name_length:1]
            collection_name_str = collection_name_bytes.decode('utf-8')

            payload = payload[collection_name_length::1]

            doc_id = payload[:DRIVER_DOCUMENT_ID_LENGTH:1]
            doc_id = doc_id.decode('utf-8')
            payload = payload[DRIVER_DOCUMENT_ID_LENGTH::]

            snapshot = payload[::]

            collection: CollectionOperations = self._db_core.get_collection_safely(collection_name_str)

            if doc_id not in collection.doc_ids():
                fake_timestamp = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
                send_timestamp(fake_timestamp)
                return

            _doc_id = DocumentId(doc_id)
            sbf_and_ph2 = collection.get_snapshot(_doc_id)
            if sbf_and_ph2 is None:
                fake_timestamp = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
                send_timestamp(fake_timestamp)
                return

            sbf, ph2 = sbf_and_ph2
            local_snapshot = Snapshot(sbf, ph2)

            if bytes(local_snapshot.get()) == snapshot:
                self._socket.sendto(AAEAnswererWorker.TERMINATION_PAYLOAD, addr_port)
                return None

            local_timestamp = collection.get_updated_at(DocumentId(doc_id))
            send_timestamp(local_timestamp)


class ActiveAntiEntropy(Subscriber):

    def __init__(self, config: AAEConfig, db_engine: DBOperationEngine):
        self._conf = config

        self._db_engine = db_engine
        self._db_core = db_engine.db_core

        self._doc_receiver = DocumentReceiver(self._conf.current.document_receiver.port)
        self._snapshot_receiver = AAEAnswererWorker(
            self._conf.current.snapshot_receiver.addr, self._conf.current.snapshot_receiver.port,
            self._db_core, self._conf.neighbors
        )

        self._document_event_queue = Queue()
        self._collection_event_queue = Queue()

        def snapshot_receiver_handler():
            while True:
                try:
                    self._snapshot_receiver.processing()
                except Exception as e:
                    logging.warning(e)
                    continue

        receiver = threading.Thread(target=snapshot_receiver_handler, args=())
        receiver.start()

        def document_receiver_handler():
            while True:
                doc_and_metadata = self._doc_receiver.get_document_and_metadata()
                if doc_and_metadata is not None:
                    collection, doc_id, doc, updated_at = self._parse_document_and_metadata(doc_and_metadata)
                    self._on_received_doc(collection, doc_id, doc, updated_at)

        doc_receiver = threading.Thread(target=document_receiver_handler, args=())
        doc_receiver.start()

    def callback(self, event: Event):
        doc_opers = [oper.value for oper in list(DocumentOperation) + list(CollectionOperation)]

        if event.event_code in doc_opers:
            self._document_event_queue.put(event)
            return

        collection_opers = [oper.value for oper in list(DocumentOperation)]
        if event.event_code in collection_opers:
            # self._collection_event_queue.put(event)
            return

    def processing(self):
        def process_queue():
            if self._document_event_queue.qsize() == 0:
                return False

            by_doc_id = dict()
            while self._document_event_queue.qsize() > 0:
                ev: DocumentOrientedEvent = self._document_event_queue.get()
                doc_id = str(ev.document_id)
                by_doc_id[doc_id] = ev

            for doc_id, ev in by_doc_id.items():
                collection = self._db_core.collections[ev.collection.name]
                self._broadcast_document(ev.document_id, collection)

            return True

        def iteration():
            process_queue()

            for collection in self._db_core.collections.values():
                doc_ids = collection.doc_ids()

                while len(doc_ids) > 0:
                    if process_queue():
                        continue

                    doc_id: DocumentId = doc_ids.pop()
                    self._broadcast(doc_id, collection)

        while True:
            try:
                iteration()
            except Exception as e:
                logging.warning(e)

    def _send_document(self, receiver_addr_port: tuple, collection: CollectionName, doc_id: DocumentId, doc: Document, updated_at: datetime):
        bytes_to_send = bytearray()
        collection_name_encoded = collection.name.encode('utf-8')
        collection_name_len = len(collection_name_encoded)
        collection_name_len_encoded = collection_name_len.to_bytes(DRIVER_COLLECTION_NAME_LENGTH_BYTES,
                                                                   DRIVER_BYTEORDER, signed=False)
        doc_id_encoded = str(doc_id).encode('utf-8')
        updated_at_encoded = datetime.strftime(updated_at, DocumentId.UTC_FORMAT).encode('utf-8')

        bytes_to_send.extend(collection_name_len_encoded)
        bytes_to_send.extend(collection_name_encoded)
        bytes_to_send.extend(doc_id_encoded)
        bytes_to_send.extend(updated_at_encoded)
        bytes_to_send.extend(doc.document.encode('utf-8'))

        send_message_to(
            receiver_addr_port,
            bytes_to_send
        )

    def _broadcast_document(self, doc_id: DocumentId, collection: CollectionOperations):
        data, updated_at = collection.read_document_with_updated_at(doc_id)
        _json = json.loads(data)

        for neigh in self._conf.neighbors:
            self._send_document(
                (neigh.document_receiver.addr, neigh.document_receiver.port),
                CollectionName(collection.name),
                doc_id,
                Document(data),
                updated_at
            )

    def _broadcast(self, doc_id: DocumentId, collection: CollectionOperations):
        sbf_and_ph2 = collection.get_snapshot(doc_id)
        sbf, ph2 = sbf_and_ph2
        snapshot = Snapshot(sbf, ph2)
        check_snapshot = AAECheckSnapshot(collection.name, str(doc_id), snapshot)
        b_check_snapshot = check_snapshot.get()

        for neigh in self._conf.neighbors:
            receiver_addr_port = (neigh.snapshot_receiver.addr, neigh.snapshot_receiver.port)

            sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

            sock.settimeout(_timeout)

            sock.sendto(b_check_snapshot,
                        receiver_addr_port
                        )

            try:
                payload, server_addr_port = sock.recvfrom(48)
            except socket.timeout:
                continue
            resp_type = AAEOperationType.get_by_value(payload[0])

            if resp_type == AAEOperationType.TERMINATE_SESSION:
                continue

            if resp_type == AAEOperationType.SENDING_TIMESTAMP:
                b_timestamp = payload[1:]
                s_timestamp = b_timestamp.decode()
                timestamp = datetime.strptime(s_timestamp, DocumentId.UTC_FORMAT)

                local_timestamp = collection.get_updated_at(doc_id)
                if local_timestamp > timestamp:
                    recv_doc_addr_port = (neigh.document_receiver.addr, neigh.document_receiver.port)
                    data, updated_at = collection.read_document_with_updated_at(doc_id)
                    self._send_document(recv_doc_addr_port, CollectionName(collection.name), doc_id, Document(data), updated_at)

    @staticmethod
    def _parse_document_and_metadata(src: bytearray):
        # FORMAT
        # |COLLECTION_NAME_LENGTH|COLLECTION_NAME|  DOC_ID  |UPDATED_AT| DOCUMENT |
        #           1byte             1-255bytes   26bytes      26bytes    Xbytes
        collection_name_length_bytes = src[:DRIVER_COLLECTION_NAME_LENGTH_BYTES:1]
        src = src[DRIVER_COLLECTION_NAME_LENGTH_BYTES::]

        collection_name_length = int.from_bytes(collection_name_length_bytes, DRIVER_BYTEORDER, signed=False)
        collection_name_bytes = src[:collection_name_length:1]
        collection_name_str = collection_name_bytes.decode('utf-8')
        collection_name = CollectionName(collection_name_str)

        src = src[collection_name_length::]

        doc_id_bytes = src[:DRIVER_DOCUMENT_ID_LENGTH:]
        doc_id = doc_id_bytes.decode('utf-8')
        doc_id = DocumentId(doc_id)

        src = src[DRIVER_DOCUMENT_ID_LENGTH::]

        UPDATED_AT_LENGTH = 26
        updated_at_bytes = src[:UPDATED_AT_LENGTH:]
        updated_at_str = updated_at_bytes.decode('utf-8')
        updated_at = datetime.strptime(updated_at_str, DocumentId.UTC_FORMAT)

        src = src[UPDATED_AT_LENGTH::]

        doc_str = src.decode('utf-8')
        doc = Document(doc_str)

        return collection_name, doc_id, doc, updated_at

    def _on_received_doc(self, collection: CollectionName, doc_id: DocumentId, doc: Document, updated_at: datetime):
        db_collection: CollectionOperations = self._db_core.get_collection_safely(collection.name)
        filename = str(doc_id)

        if not db_collection.document_exists(filename):
            db_collection.create_document(filename, doc.document, updated_at)
            return

        local_updated_at = db_collection.get_updated_at(doc_id)
        if local_updated_at >= updated_at:
            return

        db_collection.update_document(doc_id, doc.document, updated_at)
