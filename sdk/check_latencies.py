import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import argparse
import time
import uuid
import io

from google.cloud import storage

TEST_BUCKET = "ob_gcp_exploration"


def upload_download_delete_cycle(num_cycles_per_worker, profile=False, credentials=None, project_id=None):
    t = time.time()
    storage_client = storage.Client(credentials=credentials, project=project_id)
    bucket = storage_client.bucket(TEST_BUCKET)
    if profile:
        print("A: %.2f" % (time.time() - t))
    for _ in range(num_cycles_per_worker):
        blob_name = "check_latencies_" + str(uuid.uuid4())
        blob = bucket.blob(blob_name)
        q1 = time.time()
        blob.upload_from_file(io.BytesIO(b''))
        q1point5 = time.time()
        if profile:
            print("BB: %.2f" % (q1point5 - q1))
        blob.upload_from_file(io.BytesIO(b''))
        q2 = time.time()
        if profile:
            print("B: %.2f" % (q2 - q1point5))
        assert len(blob.download_as_bytes()) == 0
        q3 = time.time()
        if profile:
            print("C: %.2f" % (q3 - q2))
        blob.delete()
        q4 = time.time()
        if profile:
            print("D: %.2f" % (q4 - q3))
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

    warm_up_futures = []
    for _ in range(2 * max_workers):
        warm_up_futures.append(pool.submit(noop))
    t = time.time()
    for f in warm_up_futures:
        f.result()
    print("Warm-up complete (took %.2f seconds)" % (time.time() - t))

    import google.auth

    s = time.time()
    credentials, project_id = google.auth.default()
    print("pre-cred time = %.2f" % (time.time() -s))

    for gen in range(num_generations):
        parallelism = multiplier ** gen
        print("Processing generation %d (%d workers at a time)" % (gen, parallelism))
        futures = []
        for i in range(parallelism):
            f = pool.submit(upload_download_delete_cycle, num_cycles_per_worker, profile=bool(i == 0), credentials=credentials,project_id=project_id)
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
    parser.add_argument('--num-generations', default=5, type=int)
    parser.add_argument('--multiplier', default=2, type=int)
    parser.add_argument('--use-processes', action='store_true')
    parser.add_argument('--num-cycles-per-worker', default=10, type=int)
    args = parser.parse_args()
    do_it(args.num_generations, args.multiplier, args.use_processes, args.num_cycles_per_worker)
    return 0


if __name__ == '__main__':
    sys.exit(main())
