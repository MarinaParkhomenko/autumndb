import datetime


class DocumentId:
    UTC_FORMAT = '%Y_%m_%d_%H_%M_%S_%f'

    def __init__(self, src: str = None):
        if src is None:
            src = datetime.datetime.utcnow().strftime(DocumentId.UTC_FORMAT)

        if not DocumentId.is_valid(src):
            raise Exception(f"Document ID {src} is not valid")

        self._id = src

    def __str__(self):
        return self._id

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self._id)

    PATTERN = r'\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}_\d{6}'
    @staticmethod
    def is_valid(doc_id: str):

        import re
        match = re.match(DocumentId.PATTERN, doc_id)

        return match is not None


DOC_ID_LENGTH = len(str(DocumentId()))
