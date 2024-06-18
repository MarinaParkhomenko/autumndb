import json

from db_driver import DRIVER_BYTEORDER


class Frozen:

    def __init__(self):
        self._is_frozen = False

    def decorator(func):
        def magic(self, *args, **kwargs):
            if self._is_frozen:
                raise Exception('Is frozen for the changes')
            func(self, *args, **kwargs)

        return magic

    def froze(self):
        self._is_frozen = True

    decorator = staticmethod(decorator)


def to_bytes(obj) -> bytearray:
    if isinstance(obj, str):
        return obj.encode('utf-8')

    if isinstance(obj, int):
        return obj.to_bytes((obj.bit_length() + 7) // 8, byteorder=DRIVER_BYTEORDER)

    if isinstance(obj, list):
        return str(obj).encode('utf-8')

    if isinstance(obj, dict):
        return json.dumps(obj).encode('utf-8')

    raise Exception('Could not convert to bytes')


def to_bytearray_from_values(d: dict, acc: bytearray = None) -> bytearray:
    if acc is None:
        acc = bytearray()

    for value in d.values():
        if isinstance(value, dict):
            to_bytearray_from_values(value, acc)
        else:
            acc.extend(to_bytes(value))

    return acc
