from bisect import bisect_left

from algorithms import Frozen

BLOCK_SIZE = 8
MAX_VALUE = (1 << BLOCK_SIZE) - 1

PRIME_NUMBERS = [
    2,	3,	5,	7,	11,	13,	17,	19,	23,	29,
    31,	37,	41,	43,	47,	53, 59,	61, 67, 71,
    73,	79,	83,	89,	97,	101, 103, 107, 109, 113,
    127, 131, 137, 139, 149, 151, 157, 163, 167, 173,
    179, 181, 191, 193, 197, 199, 211, 223, 227, 229,
    233, 239, 241, 251
]


class PH2(Frozen):

    def __init__(self):
        super().__init__()
        self._bytes = bytearray()

    @Frozen.decorator
    def append(self, _bytes: bytes):
        self._bytes.extend(_bytes)

    def hashing(self) -> bytes:
        required_bytes_per_block = BLOCK_SIZE // 8

        need_to_add_bytes = required_bytes_per_block - (len(self._bytes) % required_bytes_per_block)
        need_to_add_bytes = need_to_add_bytes % required_bytes_per_block # to avoid extra adding
        self._bytes.extend([0] * need_to_add_bytes)

        bytes_len = len(self._bytes)

        def search(alist, item):
            'Locate the leftmost value exactly equal to item'
            i = bisect_left(alist, item)
            if i != len(alist) and alist[i] == item:
                return True

            return False

        sum_regular = 0
        overflow_regular = 0
        sum_primes = 0
        overflow_primes = 0
        primes_count = 0
        regular_count = 0

        for i in range(0, bytes_len, required_bytes_per_block):
            start = i
            end = i + required_bytes_per_block
            part = self._bytes[start:end:1]
            item = int.from_bytes(part, byteorder='big', signed=False)

            if search(PRIME_NUMBERS, item):
                diff = MAX_VALUE - sum_primes
                if item >= diff:
                    sum_primes = item - diff
                    overflow_primes = (overflow_primes + 1) % MAX_VALUE
                else:
                    sum_primes += item
                primes_count = (primes_count + 1) % MAX_VALUE
            else:
                diff = MAX_VALUE - sum_regular
                if item >= diff:
                    sum_regular = item - diff
                    overflow_regular = (overflow_regular + 1) % MAX_VALUE
                else:
                    sum_regular += item
                regular_count = (regular_count + 1) % MAX_VALUE

        return bytes(
            [
                regular_count % MAX_VALUE,
                primes_count % MAX_VALUE,
                sum_regular % MAX_VALUE,
                overflow_regular % MAX_VALUE,
                sum_primes % MAX_VALUE,
                overflow_primes % MAX_VALUE,
            ]
        )

    def digest(self) -> int:
        hashed = self.hashing()

        ph2_hash = 0

        def push(part):
            nonlocal ph2_hash
            part = part % MAX_VALUE
            ph2_hash <<= BLOCK_SIZE
            ph2_hash |= part

        for b in hashed:
            push(b)

        return ph2_hash
