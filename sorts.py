from functools import partial, reduce
from multiprocessing import Pool
import numpy as np


def get_digit(n, d):
    return (n // (10 ** d)) % 10


def do_sort(arg, array, d):
    left, right = arg

    array = list(filter(lambda a: left <= get_digit(a, d) < right, array))

    counts = [0] * (right - left)
    for a in array:
        digit = get_digit(a, d)
        counts[digit - left] += 1

    # accumulating
    for i in range(1, len(counts)):
        counts[i] += counts[i - 1]

    # right shifting
    for i in reversed(range(1, len(counts))):
        counts[i] = counts[i - 1]
    counts[0] = 0

    res = [0] * len(array)
    for a in array:
        digit = get_digit(a, d) - left
        index = counts[digit]
        res[index] = a
        counts[digit] += 1

    if len(res) == 0:
        return np.zeros(0, dtype=int)

    return res


def counting_sort(array, d, processors_count):
    part_size, rem = divmod(10, processors_count)

    with Pool(processes=processors_count) as pool:
        digits_distribution = [(part_size * i, (part_size * (i + 1))) for i in range(processors_count)]

        if rem > 0:
            digits_distribution[-1] = (part_size * (processors_count - 1), 10)

        results = pool.map(partial(do_sort, array=array, d=d), digits_distribution)

        return np.concatenate(results).tolist()


def radix_sort(array, max_value, processors_count):
    num_digits = 0
    while max_value > 0:
        max_value //= 10
        num_digits += 1

    for d in range(num_digits):
        array = counting_sort(
            array, d=d,
            processors_count=processors_count,
        )
    return array
