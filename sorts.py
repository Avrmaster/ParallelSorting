from functools import partial, reduce
from multiprocessing import Pool
import numpy as np


def get_digit(n, d):
    return (n // (10 ** d)) % 10


def do_count(array, d):
    counts = [0] * 10
    for a in array:
        counts[get_digit(a, d)] += 1
    return counts


def populate(target, counts, array, d):
    # array = list(filter(lambda a: left <= get_digit(a, d) < right, array))
    # print('array', array)

    res = [0] * counts[target]
    if len(res) == 0:
        return np.zeros(0, dtype=int)

    i = 0
    for a in array:
        digit = get_digit(a, d)
        if digit == target:
            res[i] = a
            i += 1

    return res


def counting_sort(array, d, processors_count):
    array_len = len(array)
    part_size, rem = divmod(array_len, processors_count)

    np_counts: np.ndarray = np.zeros(10, dtype=int)
    with Pool(processes=processors_count) as pool:
        sub_arrays = [array[(part_size * i):(part_size * (i + 1))] for i in range(processors_count)]
        if rem > 0:
            sub_arrays[0].extend(array[-rem:])

        all_counts = pool.map(partial(do_count, d=d), sub_arrays)
        for c in all_counts:
            np_counts += np.array(c)

        counts: list = np_counts.tolist()

        # # accumulating
        # for i in range(1, len(counts)):
        #     counts[i] += counts[i - 1]
        #
        # # right shifting
        # for i in reversed(range(1, len(counts))):
        #     counts[i] = counts[i - 1]
        # counts[0] = 0

        results = pool.map(partial(populate, array=array, counts=counts, d=d), range(10))
        return np.concatenate(results).tolist()

        # res = [0] * len(array)
        # for a in array:
        #     index = counts[get_digit(a, d)]
        #     res[index] = a
        #     counts[get_digit(a, d)] += 1
        # return res


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
