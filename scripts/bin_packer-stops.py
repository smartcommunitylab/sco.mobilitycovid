import argparse
import os
import time
import hashlib
import re
import io

from datetime import date, datetime, timedelta, timezone

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

import pandas as pd

from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# concurrency
NUM_THREADS = 8
# TODO implement an external journal/log
SKIP_EXISTING = False


# azure config
CONN_TIMEOUT = 300

# templates
CONN_STRING = "BlobEndpoint=https://{}.blob.core.windows.net/;SharedAccessSignature={}"
COMPRESSION = "snappy"

#
# Utils
#


def enumerate_prefixes(start=0, end=256):
    for i in range(start, end):
        yield '{:02x}'.format(i)


def read_parquet_azure(blob_service_client, container, key):
    print("read df from "+key)
    blob_client = blob_service_client.get_blob_client(container, key)
    # check if size is 0
    properties = blob_client.get_blob_properties(timeout=30)
    size = int(properties['size'])
    if(size == 0):
        raise Exception(f"Error parquet size is {size} bytes for {key}")
    blob = blob_client.download_blob(timeout=CONN_TIMEOUT)
    bytesio = io.BytesIO()
    bytesio.write(blob.readall())
    bytesio.seek(0)
    df = pd.read_parquet(bytesio, engine='pyarrow')
    del bytesio
    return df

#
# Argparser
#


def get_args():
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="BinPacker pandas")
    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument(
        "--storage", type=str, required=True, help="Azure storage")
    requiredNamed.add_argument(
        "--sas", type=str, required=True, help="SAS token")
    requiredNamed.add_argument(
        "--container-in", type=str, required=True, help="Input container")
    requiredNamed.add_argument(
        "--container-out", type=str, required=True, help="Output container")
    requiredNamed.add_argument("--path-in", required=True,
                               type=str, help="Input path")
    requiredNamed.add_argument("--path-out", required=True, type=str, default="",
                               help="Output path")
    parser.add_argument("--state", type=str, default=None,
                        help="State key")                               
    parser.add_argument("--basekey", type=str, default=None,
                        help="Output key")
    parser.add_argument("--part-num", type=int, default=0,
                        help="Output part")
    parser.add_argument("--dedup-columns", type=str, default="",
                        help="Output dedup columns")
    parser.add_argument("--compression", type=str, default=COMPRESSION,
                        help="Compression algo")
    parser.add_argument("--threads", type=int, default=NUM_THREADS,
                        help="Threads")
    parsed_args = parser.parse_args()

    return parsed_args

#
# Main function
#


def main():
    """Main function"""

    # Get args
    args = get_args()

    # Azure credentials
    sas_token = args.sas
    storage_account_name = args.storage
    container_in = args.container_in
    container_out = args.container_out

    # input paths
    path_in = args.path_in
    state = args.state
    #explode path for prefixes
    paths_in = ["{}prefix={}/state={}".format(path_in, p, state) for p in enumerate_prefixes()]

    # output path
    path_out = args.path_out
    basekey = args.basekey
    if basekey == None:
        basekey = ''.join(paths_in)
    part_num = args.part_num

    dedup_columns = args.dedup_columns

    # compression algo
    compression = args.compression

    # threads for download
    threads = args.threads

    start_time = time.time()

    # derive hash and filename
    part_key = '{:05d}'.format(part_num)
    # fix name as static hash to be reproducible
    filename_hash = hashlib.sha1(str.encode(basekey+"/"+part_key)).hexdigest()

    blob_key = "{}part-{}-{}.{}.parquet".format(
        path_out,
        part_key, filename_hash, compression)

    # init azure client
    blob_service_client = BlobServiceClient.from_connection_string(
        CONN_STRING.format(storage_account_name, sas_token))
    container_client = blob_service_client.get_container_client(
        container_in)
    blob_client = blob_service_client.get_blob_client(
        container_out, blob_key)

    # check if dest exists to skip
    skip = False

    if SKIP_EXISTING:
        print("check for "+blob_key + " in "+container_out)
        try:
            # One of the very few methods which do not mutate state
            properties = blob_client.get_blob_properties()
            if(int(properties['size']) > 0):
                # skip only if not empty
                skip = True
        except ResourceNotFoundError:
            # Not found
            skip = False
            pass

    if skip:
        print("skip "+blob_key)
    else:
        print("process "+blob_key + " to " + container_out)
        read_time = time.time()

        # read with threads
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            frames = []
            # fetch only parquets
            # TODO make configurable via param
            pattern = 'part-(.+?).parquet'
            for path in paths_in:
                print("read from "+path)
                for blob in container_client.list_blobs(name_starts_with=path):
                    key = blob.name
                    filename = key.rsplit('/', 1)[1]
                    match = re.search(pattern, filename)
                    if match:
                        #print("read from "+key)
                        future = executor.submit(
                            read_parquet_azure, blob_service_client, container_in, key)
                        futures.append(future)
            # wait for results
            for future in futures:
                kdf = future.result()
                frames.append(kdf)

        # ensure we wait all tasks
        # TODO check if all done
        done = concurrent.futures.wait(futures)

        # join
        print("concatenate '%i' dataframes..." % len(frames))
        concat_time = time.time()
        df = pd.concat(frames)

        # dedup
        dedup_time = time.time()
        if dedup_columns:
            print("deduplicate rows...")
            dcolumns = [s for s in dedup_columns.split(',')]
            df.drop_duplicates(subset=dcolumns, inplace=True)

        # write to io buffer
        print("write to buffer...")
        write_time = time.time()
        bytesio = io.BytesIO()
        # note do not write index in output, doesn't make sense
        df.to_parquet(bytesio, engine='pyarrow',
                      compression=compression, index=False)
        bytesio.seek(0)

        # check if empty
        if(len(bytesio.getvalue()) == 0):
            raise Exception(f"Error output size is 0 bytes for {blob_key}")

        # upload
        print("upload "+blob_key + " to " + container_out)
        upload_time = time.time()

        blob_client.upload_blob(bytesio, overwrite=True)

        del bytesio
        del df
        end_time = time.time()
        print("Done in {} seconds (read:{} concat:{} dedup:{} write:{} upload:{})".format(
            int(end_time - start_time),
            int(concat_time - read_time),
            int(dedup_time - concat_time),
            int(write_time - dedup_time),
            int(upload_time - write_time),
            int(end_time - upload_time)
        ))

#
# END OF CODE
#


if __name__ == "__main__":
    main()
