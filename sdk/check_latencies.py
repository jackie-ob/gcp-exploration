import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import argparse
import time
import uuid
import io
from contextlib import contextmanager

from google.cloud import storage

TEST_BUCKET = "ob_gcp_exploration"

CACHED_CLIENT = None


@contextmanager
def measure(label):
    t0 = time.time()
    splits = []

    def add_split(split_label):
        splits.append((split_label, time.time()))

    try:
        yield add_split
    finally:
        duration = time.time() - t0
        print("%s %.3f" % (label, duration))
        split_durations = [None] * len(splits)
        for i in range(len(splits)):
            if i == 0:
                split_durations[i] = splits[i][1] - t0
            else:
                split_durations[i] = splits[i][1] - splits[i - 1][1]
        for i, sd in enumerate(split_durations):
            print("    %20s: %-.2f" % (splits[i][0], split_durations[i]))


def upload_download_delete_cycles(num_cycles_per_worker, use_cached_client=False):
    if use_cached_client:
        global CACHED_CLIENT
        if not CACHED_CLIENT:
            CACHED_CLIENT = storage.Client()
        storage_client = CACHED_CLIENT
    else:
        storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)
    for i in range(num_cycles_per_worker):
        with measure("cycle_%d" % i) as m:
            blob_name = "check_latencies_" + str(uuid.uuid4())
            blob = bucket.blob(blob_name)
            blob.upload_from_file(io.BytesIO(b''))
            m("first_upload")
            blob.upload_from_file(io.BytesIO(b''))
            m("second_upload")
            assert len(blob.download_as_bytes()) == 0
            m("download")
            blob.delete()
            m("delete")


def noop():
    time.sleep(0.5)


def do_it(num_generations, multiplier, use_processes, num_cycles_per_worker):
    max_workers = multiplier ** (num_generations - 1)
    if use_processes:
        pool = ProcessPoolExecutor(max_workers=max_workers)
        use_cached_client = True
    else:
        pool = ThreadPoolExecutor(max_workers=max_workers)
        use_cached_client = False

    print("Warming up executor")

    warm_up_futures = []
    for _ in range(2 * max_workers):
        warm_up_futures.append(pool.submit(noop))
    t = time.time()
    for f in warm_up_futures:
        f.result()
    print("Warm-up complete (took %.2f seconds)" % (time.time() - t))

    for gen in range(num_generations):
        parallelism = multiplier ** gen
        print("Processing generation %d (%d workers at a time)" % (gen, parallelism))
        futures = []
        for i in range(parallelism):
            f = pool.submit(upload_download_delete_cycles, num_cycles_per_worker, use_cached_client=use_cached_client)
            futures.append(f)
        [f.result() for f in futures]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-generations', default=5, type=int)
    parser.add_argument('--multiplier', default=2, type=int)
    parser.add_argument('--use-processes', action='store_true')
    parser.add_argument('--num-cycles-per-worker', default=10, type=int)
    args = parser.parse_args()
    do_it(args.num_generations, args.multiplier, args.use_processes, args.num_cycles_per_worker)
    return 0


if __name__ == '__main__':
    sys.exit(main())
