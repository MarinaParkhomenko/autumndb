from algorithms import Frozen


class SpectralBloomFilter(Frozen):
    PRIMES = [2, 3, 5, 7, 11, 13, 17]

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._size = len(SpectralBloomFilter.PRIMES) + 1
        self._entries = [0] * self._size

    @Frozen.decorator
    def add(self, _bytes: bytes):
        for _b in _bytes:
            self._add_single(_b)

    @Frozen.decorator
    def _add_single(self, _byte: int):
        is_jocker = True
        for i, prime in enumerate(SpectralBloomFilter.PRIMES):
            if _byte % prime == 0:
                self._entries[i] = (self._entries[i] + 1) % 255
                is_jocker = False

        if is_jocker:
            self._entries[-1] = (self._entries[-1] + 1) % 255

    def get(self) -> bytes:
        return bytes(self._entries)
