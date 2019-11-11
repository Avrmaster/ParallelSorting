from numpy.random import randint
from time import time

from sorts import radix_sort, counting_sort

max_value = 999
times = {}
for proc in [1, 2, 3, 4]:
    # for arr_size in [20]:
    for arr_size in [900000]:
        arr = randint(0, max_value, size=arr_size).tolist()

        print('sorting..')
        start = time()
        # res = counting_sort(arr, 0, proc)
        res = radix_sort(arr, max_value, proc)
        end = time()

        # print('Result', res)

        delta = end - start
        times[proc] = delta
        speed_up = times[1] / delta
        efficiency = speed_up / proc

        print(f'\rSorting on {proc} processor(s) took {abs(start - end)} second(s)\n'
              f'Boost: {speed_up}x\n'
              f'Efficiency: {efficiency}\n\n')
