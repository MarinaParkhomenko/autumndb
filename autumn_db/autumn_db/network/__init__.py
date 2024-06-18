import json
import os
import socket
import threading

from autumn_db import DocumentId
from autumn_db.autumn_db import DBCoreEngine, DBOperationEngine, CreateOperation, ReadOperation, UpdateOperation, DeleteOperation
from autumn_db.event_bus.active_anti_entropy import AAEConfig, ActiveAntiEntropy
from db_driver import DRIVER_COLLECTION_NAME_LENGTH_BYTES as COLLECTION_NAME_LENGTH_BYTES, DRIVER_OPERATION_LENGTH, \
    DRIVER_DOCUMENT_ID_LENGTH, DocumentOperation
from db_driver import DRIVER_BYTEORDER as BYTEORDER
from db_driver import DocumentOperation as DBOperation
from db_driver import CollectionOperation as CollectionOperation


# MESSAGE format
# |OpCode|Collection name length|Collection name|Data   |
#  1byte        1byte               1-255bytes   Xbytes
class ClientEndpoint:
    BUFFER_SIZE = 1

    def __init__(self, port: int, db_core: DBCoreEngine):
        self._db_core = db_core

        conf = self._read_aae_config()
        self._db_opers = DBOperationEngine(db_core)
        aae = ActiveAntiEntropy(conf, self._db_opers)

        threading.Thread(target=aae.processing, args=()).start()

        th = threading.Thread(target=self._db_opers.processing, args=())
        th.start()

        self._db_opers.event_bus.subscribe(DocumentOperation.UPDATE_DOC, aae.callback)
        self._db_opers.event_bus.subscribe(DocumentOperation.CREATE_DOC, aae.callback)

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._port = port
        self._socket.bind(
            ('0.0.0.0', port)
        )
        self._socket.listen()

    @staticmethod
    def _read_aae_config() -> AAEConfig:
        filename = os.environ['AAE_CONFIG_NAME']

        with open(f'{filename}.json', 'r') as c:
            config_content = c.read()

        config = json.loads(config_content)

        res = AAEConfig(**config)
        return res

    def processing(self):
        while True:
            connection, client_address = self._socket.accept()

            received = bytearray()
            while True:
                part = connection.recv(ClientEndpoint.BUFFER_SIZE)
                if not part or part == b'\x00':
                    break

                received.extend(part)

            oper = received[0]
            received = received[DRIVER_OPERATION_LENGTH::]
            if DBOperation.CREATE_DOC.value == oper:
                collection_name_length_bytes = received[:COLLECTION_NAME_LENGTH_BYTES:1]
                received = received[COLLECTION_NAME_LENGTH_BYTES::]

                collection_name_length = int.from_bytes(collection_name_length_bytes, BYTEORDER, signed=False)
                collection_name_bytes = received[:collection_name_length:1]
                collection_name = collection_name_bytes.decode('utf-8')

                received = received[collection_name_length::]
                doc_str = received.decode('utf-8')
                oper = CreateOperation(collection_name, doc_str)
                self._db_opers.add_operation(oper)

                doc_id = oper.document_id
                response_bytes = str(doc_id).encode('utf-8')
                connection.sendall(response_bytes)

            if DBOperation.READ_DOC.value == oper:
                collection_name_length_bytes = received[:COLLECTION_NAME_LENGTH_BYTES:1]
                received = received[COLLECTION_NAME_LENGTH_BYTES::]

                collection_name_length = int.from_bytes(collection_name_length_bytes, BYTEORDER, signed=False)
                collection_name_bytes = received[:collection_name_length:1]
                collection_name = collection_name_bytes.decode('utf-8')

                received = received[collection_name_length::]
                doc_id = received.decode('utf-8')
                doc_id = DocumentId(doc_id)

                oper = ReadOperation(collection_name, doc_id)
                self._db_opers.add_operation(oper)

                while not oper.is_finished():
                    continue

                result = oper.data
                _response_bytes = bytearray(result.encode('utf-8'))
                _response_bytes.extend(b'\x00')

                connection.sendall(_response_bytes)

            if DBOperation.UPDATE_DOC.value == oper:
                oper = self._map_to_update_operation(received)
                self._db_opers.add_operation(oper)

            if DBOperation.DELETE_DOC.value == oper:
                collection_name_length_bytes = received[:COLLECTION_NAME_LENGTH_BYTES:1]
                received = received[COLLECTION_NAME_LENGTH_BYTES::]

                collection_name_length = int.from_bytes(collection_name_length_bytes, BYTEORDER, signed=False)
                collection_name_bytes = received[:collection_name_length:1]
                collection_name = collection_name_bytes.decode('utf-8')

                received = received[collection_name_length::]
                doc_id = received.decode('utf-8')
                doc_id = DocumentId(doc_id)

                oper = DeleteOperation(collection_name, doc_id)
                self._db_opers.add_operation(oper)

            if CollectionOperation.DELETE_COLLECTION.value == oper:
                collection_name_length_bytes = received[:COLLECTION_NAME_LENGTH_BYTES]
                received = received[COLLECTION_NAME_LENGTH_BYTES:]

                collection_name_length = int.from_bytes(collection_name_length_bytes, BYTEORDER, signed=False)
                collection_name_bytes = received[:collection_name_length]
                collection_name = collection_name_bytes.decode('utf-8')

                self._db_core.delete_collection(collection_name)

            connection.close()

    @staticmethod
    def _map_to_update_operation(received: bytes):
        # UPDATE MESSAGE format
        # |OpCode|Collection name length|Collection name|Document ID|   Data   |
        #  1byte        1byte               1-255bytes     26bytes     Xbytes

        collection_name_length_bytes = received[:COLLECTION_NAME_LENGTH_BYTES:1]
        received = received[COLLECTION_NAME_LENGTH_BYTES::]

        collection_name_length = int.from_bytes(collection_name_length_bytes, BYTEORDER, signed=False)
        collection_name_bytes = received[:collection_name_length:1]
        collection_name = collection_name_bytes.decode('utf-8')

        received = received[collection_name_length::]

        doc_id_bytes = received[:DRIVER_DOCUMENT_ID_LENGTH:]
        doc_id = doc_id_bytes.decode('utf-8')
        doc_id = DocumentId(doc_id)

        received = received[DRIVER_DOCUMENT_ID_LENGTH::]
        doc_str = received.decode('utf-8')

        oper = UpdateOperation(collection_name, doc_id, doc_str)
        return oper
