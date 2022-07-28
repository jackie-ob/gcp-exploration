import argparse
import statistics

from google.cloud import storage
import time
import io

TEST_BUCKET = "ob_gcp_exploration"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache', action='store_true', help="whether to use a single cached client object for the duration of the test")
    parser.add_argument('--count', default=10, type=int, help="how many upload operations to run in serial")
    args = parser.parse_args()
    durations = []

    storage_client = None
    blobs_created = []
    for i in range(args.count):
        if args.cache:
            if storage_client is None:
                storage_client = storage.Client()
        else:
            storage_client = storage.Client()
        bucket = storage_client.bucket(TEST_BUCKET)
        blob_name = "check_latencies_tmp_%d" % i
        print("Uploading empty blob %s" % blob_name)
        blobs_created.append(blob_name)
        blob = bucket.blob(blob_name)
        t = time.time()
        blob.upload_from_file(io.BytesIO(b''))
        durations.append(time.time() - t)
    print("Mean excluding first: %.2f" % statistics.mean(durations))

    print("Cleaning up")
    for b in blobs_created:
        bucket = storage_client.bucket(TEST_BUCKET)
        try:
            blob = bucket.blob(b)
            blob.delete()
        except Exception:
            pass
    print("Cleanup complete")


if __name__ == '__main__':
    main()
