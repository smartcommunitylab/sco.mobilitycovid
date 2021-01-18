from __future__ import print_function

import argparse
import hashlib
import numpy as np
import os
import pandas as pd
import time

from datetime import date, datetime, timedelta
from functools import reduce
from math import degrees

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
from pyspark.sql.window import Window

# import logging

VERSION = 2

#
# Driver settings
#
SHUFFLE_PARTITIONS = 32
CORES = "4"
RAM = "12g"
APP_NAME = "BinHours"

# always set overwrite
WRITE_MODE = "overwrite"
SKIP_EXISTING = False


# templates
TABLE_PATH = "wasbs://{}@{}.blob.core.windows.net/{}/"
CONN_STRING = "BlobEndpoint=https://{}.blob.core.windows.net/;SharedAccessSignature={}"

# need leading slash
LOCAL_PATH = "./table/"


#
# parameters
#
MAX_ACCURACY = 70

#
# Spark
#


def getSparkConfig(cores, ram, partitions, azure_accounts):
    # Setting enviroment variables and various drivers
    # "org.apache.hadoop:hadoop-azure:2.10.0"   driver Azure
    # "io.delta:delta-core_2.12:0.7.0"          driver Delta-lake
    # "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"                        configuration Delta
    # "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"     configuration Delta
    # "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.AzureLogStore"         configuration Delta

    # Set spark environments
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
    os.environ["PYSPARK_SUBMIT_ARGS"] = """--packages "org.apache.hadoop:hadoop-azure:2.10.0","io.delta:delta-core_2.12:0.7.0" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.AzureLogStore" pyspark-shell"""
    conf = (
        SparkConf()

        # SQL
        .set("spark.sql.shuffle.partitions", partitions)
        .set("spark.sql.csv.filterPushdown.enabled", "false")

        # Driver + memory
        .set("spark.driver.cores", cores)
        .set("spark.shuffle.file.buffer", "1m")
        # .set("spark.memory.offHeap.enabled","true")
        # .set("spark.memory.offHeap.size","3g")
        .set("spark.memory.fraction", "0.8")
        .set("spark.memory.storageFraction", "0.2")
        .set("spark.io.compression.lz4.blockSize", "128k")
        .set("spark.driver.maxResultSize", "0")
        .set("spark.driver.memory", ram)

        # Local storage for spilling & storing temp files
        .set("spark.local.dir", "/mnt/batch/tasks/shared")

        # Set master local
        .setMaster("local[*]")

        # App name
        .setAppName(APP_NAME)
    )

    # Azure (Keys, Filesystem WASBS)
    conf.set("spark.hadoop.fs.wasbs.impl",
             "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

    for account in azure_accounts:
        conf.set("fs.azure.sas.{}.{}.blob.core.windows.net".format(account['container'],  account['storage']),
                 account['sas'])

    return conf

#
# Utils
#


def enumerate_prefixes(start=0, end=256):
    for i in range(start, end):
        yield '{:02x}'.format(i)

#
# Argparser
#


def get_args():
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Cuebiq data processor")
    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument(
        "--storage", type=str, required=True, help="Azure storage")
    requiredNamed.add_argument(
        "--sas", type=str, required=True, help="SAS token")
    requiredNamed.add_argument(
        "--container-in", type=str, required=True, help="Input container")
    requiredNamed.add_argument(
        "--container-out", type=str, required=True, help="Output container")
    requiredNamed.add_argument("--country", type=str,
                               help="Country. Options: 'US','IT'")
    requiredNamed.add_argument(
        "--date", type=str, help="Date. Format: 'YYYY-MM-DD'")

    # optional
    parser.add_argument("--vm-cores", default=CORES,
                        type=str, help="Azure VM cores")
    parser.add_argument("--vm-ram", default=RAM,
                        type=str, help="Azure VM ram")
    parser.add_argument("--shuffle-partitions", default=SHUFFLE_PARTITIONS,
                        type=int, help="Spark shuffle partitions")
    parser.add_argument("--accuracy", type=int,
                        default=MAX_ACCURACY, help="Max accuracy")

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

    azure_accounts = list()
    azure_accounts.append({
        "storage": storage_account_name,
        "sas": sas_token,
        "container": container_in
    })
    azure_accounts.append({
        "storage": storage_account_name,
        "sas": sas_token,
        "container": container_out
    })

    # VM
    cores = args.vm_cores
    ram = args.vm_ram
    shuffle_partitions = args.shuffle_partitions

    # Date, country, prefix
    country = args.country
    date_string = args.date

    # Set date variables
    day_time = datetime.strptime(date_string, "%Y-%m-%d")
    year = day_time.year
    month = day_time.month
    day = day_time.day
    dayofyear = day_time.timetuple().tm_yday

    # config
    accuracy = args.accuracy

    # Path in - path out
    blob_in = f"wasbs://{container_in}@{storage_account_name}.blob.core.windows.net/preprocessed/{country}/"
    path_out = f"users_bin_hours3-v{VERSION}/{country}"

    # config spark
    conf = getSparkConfig(cores, ram, shuffle_partitions, azure_accounts)

    # set prop for handling partition columns as strings (fixes prefixes as int)
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    # Create spark session
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession

    # Init azure client
    blob_service_client = BlobServiceClient.from_connection_string(
        CONN_STRING.format(storage_account_name, sas_token))

    #  build keys, date is mandatory
    partition_key = "year={}/month={}/day={}".format(year, month, day)
    out_key = "year={}/dayofyear={}".format(year, dayofyear)
    blob_base = "{}/{}".format(path_out, out_key)

    # process
    print("process "+partition_key + " to " + blob_base)
    start_time = time.time()
    local_dir = LOCAL_PATH+out_key
    print("write temp to "+local_dir)

    # cleanup local if exists
    if (os.path.isdir(local_dir)):
        map(os.unlink, (os.path.join(local_dir, f)
                        for f in os.listdir(local_dir)))

    # TODO cleanup remote if exists

    # Input dataset
    print("read dataset table")
    read_time = time.time()

    # dfs = spark.read.format("parquet").load(blob_in)

    # # apply partition filter
    # dfs_partition = dfs.where(
    #     f"(year = {year} AND month = {month} AND day = {day}  AND prefix = '{prefix}')")

    # read only partition to reduce browse time
    dfs_cur_partition = spark.read.format(
        "parquet").load(f"{blob_in}/{partition_key}")

    spark_time = time.time()

    dfs_cur_partition = dfs_cur_partition.where(
        (col('accuracy') <= accuracy) & (col('accuracy') >= 0))
    dfs_cur_partition = dfs_cur_partition.withColumn(
        'hour', F.hour('timestamp'))

    result_df = dfs_cur_partition.groupBy('userId').agg(
        F.countDistinct("hour").cast('int').alias('num_hours'))

    # rebuild prefix
    result_df = result_df.withColumn('prefix', result_df.userId.substr(1, 2))
    # lit partition columns in output
    result_df = result_df.withColumn('year', F.lit(year))
    #result_df = result_df.withColumn('month', F.lit(month))
    #result_df = result_df.withColumn('day', F.lit(day))
    #result_df = result_df.withColumn('dayofyear', F.lit(dayofyear))

    # write as single partition
    result_df.repartition(1).write.partitionBy("prefix").format(
        'parquet').mode("overwrite").save(local_dir+"/")

    # stats - enable only for debug!
    # num_records = result_df.count()
    # print(f"written {num_records} rows to "+local_dir)

    # if num_records == 0:
    #     raise Exception("Zero rows output")

    print("upload local data to azure")
    upload_time = time.time()

    # upload parts over states
    for fprefix in enumerate_prefixes():
        print(f"upload files for {fprefix}")
        prefix_dir = local_dir+"/prefix="+fprefix
        prefix_key = f"{out_key}/prefix={fprefix}/"
        prefix_blob = f"{blob_base}/prefix={fprefix}"

        if (os.path.isdir(prefix_dir)):
            files = [filename for filename in os.listdir(
                prefix_dir) if filename.startswith("part-")]

            if len(files) > 0:

                for file_local in files:
                    file_path = prefix_dir+"/"+file_local
                    part_num = int(file_local.split('-')[1])
                    part_key = '{:05d}'.format(part_num)
                    # fix name as static hash to be reproducible
                    filename_hash = hashlib.sha1(
                        str.encode(prefix_key+part_key)).hexdigest()

                    blob_key = "{}/part-{}-{}.snappy.parquet".format(
                        prefix_blob, part_key, filename_hash)

                    print("upload " + file_path + " to " +
                          container_out+":"+blob_key)

                    blob_client = blob_service_client.get_blob_client(
                        container_out, blob_key)

                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)

                    # cleanup
                    os.remove(file_path)
            else:
                print(f"no files to upload for {fprefix}")

        else:
            print(f"missing partition for {fprefix}")

    print("--- {} seconds elapsed ---".format(int(time.time() - start_time)))
    print()
    shutdown_time = time.time()
    spark.stop()

    end_time = time.time()
    print("Done in {} seconds (read:{} spark:{} upload:{} shutdown:{})".format(
        int(end_time - start_time),
        int(spark_time - read_time),
        int(upload_time - spark_time),
        int(shutdown_time - upload_time),
        int(end_time - shutdown_time)
    ))
    print('Done.')
    #
    # END OF CODE
    #


if __name__ == "__main__":
    main()
