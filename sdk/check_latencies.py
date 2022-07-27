import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import argparse
import time
import uuid
import io

from google.cloud import storage

TEST_BUCKET = "ob_gcp_exploration"


def upload_download_delete_cycle(num_cycles_per_worker):
    t = time.time()
    storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)
    for _ in range(num_cycles_per_worker):
        blob_name = "check_latencies_" + str(uuid.uuid4())
        blob = bucket.blob(blob_name)
        blob.upload_from_file(io.BytesIO(b''))
        assert len(blob.download_as_bytes()) == 0
        blob.delete()
    return time.time() - t


def noop():
    time.sleep(0.5)


def do_it(num_generations, multiplier, use_processes, num_cycles_per_worker):
    max_workers = multiplier ** (num_generations - 1)
    if use_processes:
        pool = ProcessPoolExecutor(max_workers=max_workers)
    else:
        pool = ThreadPoolExecutor(max_workers=max_workers)

    print("Warming up executor")
    for _ in range(2 * max_workers):
        pool.submit(noop)
    print("Warm-up complete")

    for gen in range(num_generations):
        parallelism = multiplier ** gen
        print("Processing generation %d (%d workers at a time)" % (gen, parallelism))
        futures = []
        for i in range(parallelism):
            f = pool.submit(upload_download_delete_cycle, num_cycles_per_worker)
            futures.append(f)
        latencies = [f.result() for f in futures]
        mean = statistics.mean(latencies)
        stdev = -1
        if len(latencies) > 1:
            stdev = statistics.stdev(latencies)
        mean_per_operation = mean / (num_cycles_per_worker * 3)
        print("Gen %d (x %d): mean=%.3f stdev=%.3f mean_per_operation=%.3f" % (
        gen, parallelism, mean, stdev, mean_per_operation))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-generations', default=5)
    parser.add_argument('--multiplier', default=2)
    parser.add_argument('--use-processes', action='store_true')
    parser.add_argument('--num-cycles-per-worker', default=10)
    args = parser.parse_args()
    do_it(args.num_generations, args.multiplier, args.use_processes, args.num_cycles_per_worker)
    return 0


if __name__ == '__main__':
    sys.exit(main())
