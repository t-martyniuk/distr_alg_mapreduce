import multiprocessing

def my_mapper(array, map_results, left_idx, right_idx):
    """Function that maps every number in the array to the tuple (number, 1).

    Args:
        array: The input array.
        map_results: The list that we write tuples to.
        left_idx: The index of the array we start with.
        right_idx: The index of the array we end with.
    """
    for i in range(left_idx, right_idx):
        map_results[i] = (array[i], 1)


def my_reducer(array, reduce_results):
    """Function that calculates the number of occurrences of the numbers in the array.

    Args:
        array: The array of tuples (number, 1).
        reduce_results: The dictionary that containts numbers as keys and number of occurences of them as values.
    """
    for i in range(len(array)):
        if array[i][0] in reduce_results.keys():
            reduce_results[array[i][0]] += 1
        else:
            reduce_results[array[i][0]] = 1

#Parallel map implementation
def parallel_map(array):
    jobs = []
    map_results = multiprocessing.Manager().list([None] * len(array))     #The list that we will be writing to
    for i in range(core_number):
        left_idx = split_len * i
        right_idx = split_len * (i + 1)
        p = multiprocessing.Process(target=my_mapper, args=(array, map_results, left_idx, right_idx))
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()
    for p in jobs:
        p.terminate()

    return map_results

#Parallel reduce implementation
def parallel_reduce(array):
    # The argument array here is list of lists, alread prepared after shuffling for separate reducers.
    jobs = []

    reduce_results = multiprocessing.Manager().dict()     #The dictionary that we will be writing to
    for i in range(core_number):
        p = multiprocessing.Process(target=my_reducer, args=(array[i], reduce_results))
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()
    for p in jobs:
        p.terminate()

    return reduce_results


my_array = [2, 4, 3, 5, 2, 4, 1, 2]
print('The array is', my_array)

core_number = multiprocessing.cpu_count()
if len(my_array) > core_number:
    split_len = int(len(my_array) / core_number)
else:
    core_number = len(my_array)
    split_len = 1

mapped_list = parallel_map(my_array)
print('The result after mapping is', mapped_list)

# Shuffling
shuffled = [None] * core_number
for i in range(len(mapped_list)):
    idx = mapped_list[i][0] % 4 #shuffling by the residue of division by 4
    if shuffled[idx] == None:
        shuffled[idx] = [mapped_list[i]]
    else:
        shuffled[idx].append(mapped_list[i])

print('The result after shuffling is', shuffled)

reduced_list = parallel_reduce(shuffled)

print('The result after reducing is', reduced_list)


