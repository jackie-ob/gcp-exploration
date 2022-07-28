import argparse
import os
import uuid

from google.cloud import storage
from random import randbytes
from measure import measure
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

TEST_BUCKET = "ob_gcp_exploration"


def upload_one(name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)
    blob = bucket.blob(name)
    with open(name, 'rb') as f:
        blob.upload_from_file(f)

# This is really slow... but fast enough, so don't care
def generate_randbytes(size: int):
    result = bytearray()
    size_remaining = size
    batch_size = 10 * 1024 * 1024
    while size_remaining > 0:
        if size_remaining >= batch_size:
            result.extend(randbytes(batch_size))
            size_remaining -= batch_size
        else:
            result.extend(randbytes(size_remaining))
            size_remaining = 0
    return result

def just_do_it(total_size_mb, splits, use_processes):
    assert splits <= 32
    assert total_size_mb < 1024 * 12  # cap at 12GB... we will keep everything in memory
    total_size = total_size_mb * 1024 * 1024
    contents = []
    size_per_chunk = total_size // splits
    remainder_size = total_size % splits
    blob_prefix = str(uuid.uuid4())
    with measure("prepare_data"):
        for i in range(splits):
            blob_name = "%s_%d" % (blob_prefix, i)
            if i < remainder_size:
                data = generate_randbytes(size_per_chunk + 1)
            else:
                data = generate_randbytes(size_per_chunk)
            with open(blob_name, 'wb') as f:
                f.write(data)
            contents.append(blob_name)

    validate_total_size = sum(os.path.getsize(x) for x in contents)

    assert validate_total_size == total_size
    print("Total size generated = %d" % total_size)

    with measure("composite_upload", work_qty=total_size_mb, work_unit='MB'):
        with measure("upload_components"):
            if use_processes:
                pool = ThreadPoolExecutor(max_workers=splits)
            else:
                pool = ProcessPoolExecutor(max_workers=splits)
            futures = []
            for blob_name in contents:
                f = pool.submit(upload_one, blob_name)
                futures.append(f)
            for f in futures:
                f.result()

        with measure("compose"):
            storage_client = storage.Client()
            bucket = storage_client.bucket(TEST_BUCKET)
            compose_blob = bucket.blob(blob_prefix)
            compose_blob.compose([bucket.blob(c) for c in contents])

    with measure("cleanup"):
        storage_client = storage.Client()
        bucket = storage_client.bucket(TEST_BUCKET)
        for c in contents:
            os.remove(c)
            bucket.blob(c).delete()
        compose_blob = bucket.blob(blob_prefix)
        compose_blob.delete()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--total-size-mb', type=int, required=True)
    parser.add_argument('--splits', type=int, default=32)
    parser.add_argument('--use-processes', action='store_true')
    args = parser.parse_args()
    just_do_it(args.total_size_mb, args.splits, args.use_processes)


if __name__ == '__main__':
    main()