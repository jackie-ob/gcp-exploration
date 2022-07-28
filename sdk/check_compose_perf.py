import argparse
import uuid

from google.cloud import storage
import io
from random import randbytes
from measure import measure
from concurrent.futures import ThreadPoolExecutor

TEST_BUCKET = "ob_gcp_exploration"


def upload_one(name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)
    blob = bucket.blob(name)
    blob.upload_from_file(io.BytesIO(data))

# This is really slow... but fast enough, so don't care
def generate_randbytes(size: int):
    result = bytearray()
    size_remaining = size
    while size_remaining > 0:
        if size_remaining >= 1024 * 1024:
            result.extend(randbytes(1024 * 1024))
            size_remaining -= 1024 * 1024
        else:
            result.extend(randbytes(size_remaining))
            size_remaining = 0
    return result

def just_do_it(total_size_mb, splits):
    assert splits <= 32
    assert total_size_mb < 1024 * 12  # cap at 12GB... we will keep everything in memory
    total_size = total_size_mb * 1024 * 1024
    contents = []
    size_per_chunk = total_size // splits
    remainder_size = total_size % splits
    with measure("prepare_data"):
        for i in range(splits):
            if i < remainder_size:
                contents.append(generate_randbytes(size_per_chunk + 1))
            else:
                contents.append(generate_randbytes(size_per_chunk))

    validate_total_size = sum(len(x) for x in contents)
    blob_prefix = str(uuid.uuid4())

    assert validate_total_size == total_size
    print("Total size generated = %d" % total_size)
    component_names = []

    with measure("composite_upload", work_qty=total_size_mb, work_unit='MB'):
        with measure("upload_components"):
            pool = ThreadPoolExecutor(max_workers=splits)
            futures = []
            for i, b in enumerate(contents):
                blob_name = "%s_%d" % (blob_prefix, i)
                component_names.append(blob_name)
                f = pool.submit(upload_one, blob_name, b)
                futures.append(f)
            for f in futures:
                f.result()

        with measure("compose"):
            storage_client = storage.Client()
            bucket = storage_client.bucket(TEST_BUCKET)
            compose_blob = bucket.blob(blob_prefix)
            compose_blob.compose([bucket.blob(c) for c in component_names])

    with measure("cleanup"):
        storage_client = storage.Client()
        bucket = storage_client.bucket(TEST_BUCKET)
        for c in component_names:
            bucket.blob(c).delete()
        compose_blob = bucket.blob(blob_prefix)
        compose_blob.delete()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--total-size-mb', type=int, required=True)
    parser.add_argument('--splits', type=int, default=32)
    args = parser.parse_args()
    just_do_it(args.total_size_mb, args.splits)


if __name__ == '__main__':
    main()