import io
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from google.cloud import storage

from random import randbytes
import time
import sys

BLOB_ENDPOINT = "https://obbenchmark1.blob.core.windows.net/"


class Measure(object):
    def __init__(self, name, data_size=None):
        self._name = name
        self._start_time = None
        self._end_time = None
        self._data_size = data_size
        self._duration = None

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self._end_time = time.time()
        self._duration = self._end_time - self._start_time
        if self._data_size is None:
            print("[%s] took %.2fs" % (self._name, self._duration))
        else:
            print("[%s] took %.2fs (%.2f/s)" % (self._name, self._duration, self._data_size / self._duration))

    @property
    def rate(self):
        return self._data_size / self._duration


# This is really slow... but fast enough, so don't care
def generate_randbytes(size_mb: int):
    result = bytearray()
    for _ in range(size_mb):
        result.extend(randbytes(1024 * 1024))
    return result


SIZE_MB = 500
PARALLEL_BLOB_OPERATIONS = 10
TEST_BLOB_FN = lambda n: "test-blob-%d" % n
TEST_BUCKET = "ob_gcp_exploration"


def simple_download(i):
    with Measure('download', SIZE_MB) as m:
        storage_client = storage.Client()
        bucket = storage_client.bucket(TEST_BUCKET)
        print("Downloading blob %s" % TEST_BLOB_FN(i))
        blob = bucket.blob(TEST_BLOB_FN(i))
        print("%d bytes downloaded" % len(blob.download_as_bytes()))
    return m.rate


def simple_upload(i):
    data = bytes(generate_randbytes(SIZE_MB))
    with Measure('upload', SIZE_MB) as m:
        storage_client = storage.Client()
        bucket = storage_client.bucket(TEST_BUCKET)
        print("Uploading to blob %s" % TEST_BLOB_FN(i))
        blob = bucket.blob(TEST_BLOB_FN(i))
        upload_result = blob.upload_from_file(io.BytesIO(data))
        print("Upload result = " + str(upload_result))
    return m.rate


def main():
    executor = ThreadPoolExecutor(max_workers=PARALLEL_BLOB_OPERATIONS)
    ups = []
    for i in range(PARALLEL_BLOB_OPERATIONS):
        ups.append(executor.submit(simple_upload, i))
    up_rates = [up.result() for up in ups]
    # If task executions become staggered, this aggregation will become over optimistic.
    # No staggering observed to date, so keeping it dead simple for now
    # TODO(jackie) make it robust
    up_msg = "Aggregate upload rate: %.2f Mbps" % (sum(up_rates) * 8)
    downs = []
    for i in range(PARALLEL_BLOB_OPERATIONS):
        downs.append(executor.submit(simple_download, i))
    down_rates = [down.result() for down in downs]
    # If task executions become staggered, this aggregation will become over optimistic.
    # No staggering observed to date, so keeping it dead simple for now
    # TODO(jackie) make it robust
    down_msg = "Aggregate download rate: %.2f Mbps" % (sum(down_rates) * 8)

    print(up_msg)
    print(down_msg)


if __name__ == '__main__':
    sys.exit(main())
