import os

from autumn_db.data_storage.data_access import DataAccess


class FilesystemAccess(DataAccess):

    def create(self, pathname: str, data: str):
        if os.path.exists(pathname):
            raise RuntimeError(f"File {pathname} already exists")

        with open(pathname, 'w') as f:
            f.write(data)

    def read(self, pathname: str) -> str:
        if not os.path.exists(pathname):
            raise RuntimeError(f"Could not read {pathname}. File does not exist")

        with open(pathname, 'r') as f:
            data = f.read()

        return data

    def update(self, pathname: str, data: str):
        if not os.path.exists(pathname):
            raise RuntimeError(f"Could not read {pathname}. File does not exist")

        with open(pathname, 'w') as f:
            f.write(data)

    def delete(self, pathname: str):
        os.remove(pathname)
