# Read all files in a directory recursively in parallel

import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

CAUSE_CRASH = True

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def read_file(file):
    print(file)
    with open(file, 'rb') as f:
        x=f.read()
        #print("File: " + file)
        #print("Size of file is :", sizeof_fmt(int(f.tell())))

def read_parallel(path):
    files = []
    print(f"Reading files in parallel {path}")
    if CAUSE_CRASH:
        ls_done = 0
        for root, dirs, files in os.walk(path):
            files = [os.path.join(root, file) for file in files]
            ls_done += 1
            if ls_done % 10 == 0:
                print("ls done: ", ls_done)
        print("Got files: ", len(files))
    done = 0
    for root, dirs, files in os.walk(path): # Uses a generator
        with ThreadPoolExecutor(max_workers=64) as executor:
            futures = [executor.submit(read_file, os.path.join(root, file)) for file in files]
            for future in as_completed(futures):
                done += 1
                if done % 10 == 0:
                    print("Reads done: ", done)
                print(future.result())

if __name__ == '__main__':
    # s3://fah-public-data-covid19-cryptic-pockets mounted at /covid
    read_parallel('/covid')
