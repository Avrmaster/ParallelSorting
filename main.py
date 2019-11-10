from multiprocessing import Pool
from numpy.random import randint
from functools import partial
from time import time
import operator

from sorts import radix_sort


def do_merge(all_results):
    pointers = [0] * len(all_results)
    lengths = [len(r) for r in all_results]

    res = [None] * len(all_results)
    values = [a[p] for (p, a) in zip(pointers, all_results)]
    while True:
        min_index, min_value = min(enumerate(values), key=operator.itemgetter(1))
        pointers[min_index] += 1

        if pointers[min_index] >= lengths[min_index]:
            values.pop(min_index)
            if len(values) == 0:
                break
        else:
            values[min_index] = all_results[min_index][pointers[min_index]]

        res.append(min_value)
    return res


def do_paralil(array, sort_function, processors_count):
    array_len = len(array)
    part_size, rem = divmod(array_len, processors_count)

    with Pool(processes=processors_count) as pool:
        sub_arrays = [array[(part_size * i):(part_size * (i + 1))] for i in range(processors_count)]
        if rem > 0:
            sub_arrays.append(array[part_size * processors_count - rem:array_len])

        max_value = max(array)
        return do_merge(pool.map(func=partial(sort_function, max_value=max_value), iterable=sub_arrays))


times = {}
max_val = 9999
for proc in range(1, 17):
    # for arr_size in [1000000]:
    for arr_size in [20]:
        arr = randint(0, max_val, size=arr_size).tolist()

        start = time()
        res = do_paralil(arr, radix_sort, proc)
        end = time()

        print("Result:", res)

        delta = end - start
        times[proc] = delta

        speed_up = times[1] / delta
        efficiency = speed_up / proc

        print(f'Sorting on {proc} processor(s) took {abs(start - end)} second(s)\n'
              f'Boost: {speed_up}x\n'
              f'Efficiency: {efficiency}\n\n')
