import threading
from collections import defaultdict

# define the number of map and reduce threads
NUM_MAP_THREADS = 4
NUM_REDUCE_THREADS = 2

# define the input file and output file paths
INPUT_FILE_PATH = "input.txt"
OUTPUT_FILE_PATH = "output.txt"

# define the map and reduce functions
def map_func(chunk):
    word_count = defaultdict(int)
    for word in chunk.split():
        word_count[word] += 1
    return word_count.items()

def reduce_func(key, values):
    return key, sum(values)

# define the map thread worker function
def map_thread_worker(chunk, intermediate):
    for key, value in map_func(chunk):
        intermediate[key].append(value)

# define the reduce thread worker function
def reduce_thread_worker(key_values, output):
    for key, values in key_values:
        output.append(reduce_func(key, values))

# read the input file and split it into chunks
with open(INPUT_FILE_PATH, "r") as f:
    input_data = f.read()
    chunk_size = len(input_data) // NUM_MAP_THREADS
    chunks = [input_data[i:i+chunk_size] for i in range(0, len(input_data), chunk_size)]

# create a pool of map threads and process the chunks
intermediate = defaultdict(list)
map_threads = []
for i in range(NUM_MAP_THREADS):
    t = threading.Thread(target=map_thread_worker, args=(chunks[i], intermediate))
    map_threads.append(t)
    t.start()

for t in map_threads:
    t.join()

# shuffle the intermediate key-value pairs
shuffled = defaultdict(list)
for key, values in intermediate.items():
    for i in range(NUM_REDUCE_THREADS):
        shuffled[(key, i)].extend(values[i::NUM_REDUCE_THREADS])

# create a pool of reduce threads and process the shuffled key-value pairs
output = []
reduce_threads = []
for i in range(NUM_REDUCE_THREADS):
    t = threading.Thread(target=reduce_thread_worker, args=(shuffled.items(), output))
    reduce_threads.append(t)
    t.start()

for t in reduce_threads:
    t.join()

# write the output to a file
with open(OUTPUT_FILE_PATH, "w") as f:
    for key, value in sorted(output):
        f.write(f"{key}: {value}\n")
