from __future__ import print_function
from sklearn.cluster import DBSCAN

import argparse
import hashlib
import os
import time

from datetime import date, datetime, timedelta
from functools import reduce
from math import degrees

from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobBlock
from azure.core.exceptions import ResourceNotFoundError

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import StorageLevel
from pyspark.sql.functions import lag, pandas_udf, PandasUDFType

# import logging

VERSION = 6

#
# Driver settings
#
SHUFFLE_PARTITIONS = 32
OUT_PARTITIONS = 2
CORES = "4"
RAM = "12g"
APP_NAME = "StopUserClusters"

# always set overwrite
WRITE_MODE = "overwrite"
SKIP_EXISTING = False
THREADS = 32


# templates
TABLE_PATH = "wasbs://{}@{}.blob.core.windows.net/{}/"
CONN_STRING = "BlobEndpoint=https://{}.blob.core.windows.net/;SharedAccessSignature={}"

# need leading slash
LOCAL_PATH = "./table/"


#
# Stop locations parameters
#
EVENTS_ROAM_DIST = 70  # meters
STOPS_ROAM_DIST = 65
EARTH_RADIUS = 6372.795 * 1000
MIN_STAY = 5

US_STATES = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS',
             'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']


def read_multiple_df(spark, paths, format="parquet"):
    dfs = None
    dfs_array = []
    for path in paths:
        dfs_load = spark.read.format(format).load(path)
        dfs_array.append(dfs_load)
    dfs = reduce(DataFrame.unionAll, dfs_array)
    return dfs


#
# Stop location lib
#


def add_distance_column(dfs, order_column='timestamp'):
    # Radians lat/lon
    dfs = dfs.withColumn('latitude2', F.radians('latitude')).withColumn(
        'longitude2', F.radians('longitude'))

    # Groups GPS locations into chucks. A chunk is formed by groups of points that are distant no more than roam_dist
    w = Window.partitionBy(['userID']).orderBy(order_column)
    dfs = dfs.withColumn('next_lat', F.lead('latitude2', 1).over(w))
    dfs = dfs.withColumn('next_lon', F.lead('longitude2', 1).over(w))

    # Haversine distance
    dfs = dfs.withColumn('distance_next', EARTH_RADIUS * 2 * F.asin(F.sqrt(
        F.pow(F.sin((col('next_lat') - col('latitude2')) / 2.0), 2) + F.cos('latitude2') * F.cos('next_lat') * F.pow(
            F.sin((col('next_lon') - col('longitude2')) / 2.0), 2))))
    dfs = dfs.withColumn('distance_prev', F.lag('distance_next', default=0).over(w)).drop(
        'latitude2').drop('longitude2').drop('next_lon').drop('next_lat').drop('distance_next')
    return dfs


def get_destinations(dfs, roam_dist=110, earth_radius=6372.795 * 1000):
    """
    Applies DBSCAN to extract the unique stop locations from a pyspark DataFrame

    :param x: DataFrame with ['id_client', 'latitude', 'longitude', "from", "to"]. Coordinates are in degrees.
    :param roam_dist: The stop location size in meters.
    :param earth_radius: The radius of the earth.
    :param group_results: If True, it groups by the cluster's location and id_client.
    :return: (pyspark DataFrame) If group_results=True: ['id_client', 'clatitude', 'clongitude', 'time_spent', 'frequency']
            (pyspark DataFrame) If group_results=False: ['id_client', 'latitude', 'longitude', 'clatitude', 'clongitude', 'from', 'to']
    """

    @pandas_udf("userId string, state string, latitude double, longitude double, begin timestamp, end timestamp, clusterId integer", PandasUDFType.GROUPED_MAP)
    def get_destinations(df):
        """
        Applies DBSCAN to stop locations

        :param x: 2D numpy array with latitude and longitude.
        :param from_to_array: 2D numpy array with from and to timestamps.
        :param roam_dist: The stop location size in meters.
        :param earth_radius: The radius of the earth.
        :return: (pandas DataFrame) ['latitude', 'longitude', 'clatitude', 'clongitude', 'from', 'to', 'time_spent']
        """
        db = DBSCAN(eps=roam_dist/earth_radius, min_samples=1,
                    algorithm='ball_tree', metric='haversine')
        df["clusterId"] = db.fit_predict(df[['latitude', 'longitude']])

        return df

    dfs = dfs.withColumn('latitude', F.radians('latitude'))
    dfs = dfs.withColumn('longitude', F.radians('longitude'))

    stops_dfs = dfs.groupby('userId', 'state').apply(get_destinations)

    stops_dfs = stops_dfs.withColumn('latitude', F.degrees('latitude'))
    stops_dfs = stops_dfs.withColumn('longitude', F.degrees('longitude'))

    w = Window().partitionBy('userId', 'clusterId')

    stops_dfs = stops_dfs.withColumn(
        'clusterLatitude', F.mean('latitude').over(w))
    stops_dfs = stops_dfs.withColumn(
        'clusterLongitude', F.mean('longitude').over(w))

    stops_dfs = stops_dfs.drop('latitude').drop('longitude')

    return stops_dfs

#
# Spark
#


def getSparkConfig(cores, ram, partitions, azure_accounts, azure_oauth):
    # Setting enviroment variables and various drivers
    # "org.apache.hadoop:hadoop-azure:2.10.0"   driver Azure
    # "io.delta:delta-core_2.12:0.7.0"          driver Delta-lake
    # "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"                        configuration Delta
    # "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"     configuration Delta
    # "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.AzureLogStore"         configuration Delta

    # Set spark environments
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
    # os.environ["PYSPARK_SUBMIT_ARGS"] = """--packages "org.apache.hadoop:hadoop-azure:3.2.1" pyspark-shell"""
    os.environ["PYSPARK_SUBMIT_ARGS"] = """--packages "org.apache.hadoop:hadoop-azure:2.10.0" --jars "/mnt/batch/tasks/shared/sco-mobilitycovid-udf_2.11-1.0.jar","/mnt/batch/tasks/shared/geo-0.7.7.jar" pyspark-shell"""
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

    if azure_oauth:
        conf.set("spark.hadoop.fs.azure.account.auth.type", "OAuth")
        conf.set("spark.hadoop.fs.azure.account.oauth.provider.type",
                 "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        conf.set("spark.hadoop.fs.azure.account.oauth2.client.id",
                 azure_oauth['client-id'])
        conf.set("spark.hadoop.fs.azure.account.oauth2.client.secret",
                 azure_oauth['client-secret'])
        conf.set("spark.hadoop.fs.azure.account.oauth2.client.endpoint",
                 azure_oauth['endpoint'])
    return conf

#
# Utils
#


def enumerate_prefixes(start=0, end=256):
    for i in range(start, end):
        yield '{:02x}'.format(i)


def upload_blob(blob_service_client, container_out, blob_key, file_path):
    blob_client = blob_service_client.get_blob_client(
        container_out, blob_key)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # cleanup
    os.remove(file_path)

    return blob_key

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
        "--oauth-login", type=str, required=True, help="Oauth login")
    requiredNamed.add_argument(
        "--oauth-client-id", type=str, required=True, help="Oauth client id")
    requiredNamed.add_argument(
        "--oauth-client-secret", type=str, required=True, help="Oauth client secret")
    requiredNamed.add_argument(
        "--container-in", type=str, required=True, help="Input container")
    requiredNamed.add_argument(
        "--container-out", type=str, required=True, help="Output container")
    requiredNamed.add_argument("--country", type=str,
                               help="Country. Options: 'US','IT'")
    requiredNamed.add_argument("--prefix", type=str, help="User prefix")

    # optional
    parser.add_argument("--vm-cores", default=CORES,
                        type=str, help="Azure VM cores")
    parser.add_argument("--vm-ram", default=RAM,
                        type=str, help="Azure VM ram")
    parser.add_argument("--shuffle-partitions", default=SHUFFLE_PARTITIONS,
                        type=int, help="Spark shuffle partitions")
    parser.add_argument("--roam-dist-stops", type=int,
                        default=STOPS_ROAM_DIST, help="Roam dist stops")
    parser.add_argument("--roam-dist-events", type=int,
                        default=EVENTS_ROAM_DIST, help="Roam dist events")
    parsed_args = parser.parse_args()

    return parsed_args


#
# Main function
#
def main():
    """Main function"""

    # Get args
    args = get_args()

    # container
    container_in = args.container_in
    container_out = args.container_out

    # Azure credentials
    sas_token = args.sas
    storage_account_name = args.storage
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

    oauth_login = args.oauth_login
    oauth_client_id = args.oauth_client_id
    oauth_client_secret = args.oauth_client_secret

    # requires hadoop 3.2+
    # azure_oauth = {
    #     "endpoint": oauth_login,
    #     "client-id": oauth_client_id,
    #     "client-secret": oauth_client_secret
    # }
    azure_oauth = False

    # VM
    cores = args.vm_cores
    ram = args.vm_ram
    shuffle_partitions = args.shuffle_partitions

    # Date, prefix
    country = args.country
    prefix = args.prefix

    # process config
    roam_dist_stops = args.roam_dist_stops
    roam_dist_events = args.roam_dist_events

    # Path in - path out
    blob_in = f"wasbs://{container_in}@{storage_account_name}.blob.core.windows.net/stoplocation-v8_prefix_r70-s5-a70-h6/{country}/"
    timezones_in = f"wasbs://cuebiq-data@{storage_account_name}.blob.core.windows.net/utils_states_timezones/"
    if azure_oauth:
        # we can leverage abfss
        blob_in = f"abfss://{container_in}@{storage_account_name}.dfs.core.windows.net/stoplocation-v8_prefix_r70-s5-a70-h6/country={country}/"
        timezones_in = f"abfss://cuebiq-data@{storage_account_name}.dfs.core.windows.net/utils_states_timezones/"

    path_out_distinct = f"distinct_user_clusters-v8_r70-s5-a70-h6_clustered_{roam_dist_stops}m_v{VERSION}/country={country}"
    path_out_all = f"all_user_clusters-v8_r70-s5-a70-h6_clustered_{roam_dist_stops}m_v{VERSION}/country={country}"

    # config spark
    conf = getSparkConfig(cores, ram, shuffle_partitions,
                          azure_accounts, azure_oauth)

    # set prop for handling partition columns as strings (fixes prefixes as int)
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    # Create spark session
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    # register UDF from jar
    spark.udf.registerJavaFunction(
        "geohash", "it.smartcommunitylab.sco.mobilitycovid.udf.GeohashEncode")

    # Init azure client
    blob_service_client = BlobServiceClient.from_connection_string(
        CONN_STRING.format(storage_account_name, sas_token))

    #  build keys, date is mandatory, prefix opt
    partition_key = f"prefix={prefix}"

    print("process "+partition_key)
    start_time = time.time()
    local_dir = LOCAL_PATH+partition_key
    print("write temp to "+local_dir)

    # cleanup local if exists
    if (os.path.isdir(local_dir)):
        map(os.unlink, (os.path.join(local_dir, f)
                        for f in os.listdir(local_dir)))

    # Input dataset
    print("read dataset table")
    read_time = time.time()

    # explode days manually
    dates = [
        datetime(2020, 1, 1) + timedelta(days=x) for x in range(0, 258)]
    blobs_in = ["{}/year={}/month={}/day={}/prefix={}".format(
        blob_in, d.year, d.month, d.day, prefix) for d in dates]

    #dfs = spark.read.format("parquet").load(*blobs_in)
    dfs = read_multiple_df(spark, blobs_in)
    dfs_timezones = spark.read.format("parquet").load(timezones_in)

    # manually inject prefix column
    dfs = dfs.withColumn("prefix", F.lit(prefix))

    # apply partition filter
    dfs_state = dfs.where(f"prefix = '{prefix}'")

    print("processing with spark")
    spark_time = time.time()

    w = Window().partitionBy('userId').orderBy('begin')

    dfs_state = add_distance_column(dfs_state, order_column='begin')
    dfs_state = dfs_state.fillna(0, subset=['next_travelled_distance'])
    dfs_state = dfs_state.withColumn('lag_next_travelled_distance', F.lag(
        col('next_travelled_distance')).over(w))
    dfs_state = dfs_state.withColumn('lag_end', F.lag('end').over(w))
    dfs_state = dfs_state.withColumn('rn', F.when(((col('lag_next_travelled_distance') != col('prev_travelled_distance')) |
                                                   (col('prev_travelled_distance') > 0) |
                                                   (col('lag_next_travelled_distance') > 0) |
                                                   (col('distance_prev') > roam_dist_events) |
                                                   ((F.dayofyear(col('begin')) - F.dayofyear(col('lag_end')) == 1) &
                                                    (F.hour(col('begin')) < 6))
                                                   ) &
                                                  ((col('lag_end').isNull()) | (col('lag_end') < col('begin'))), 1).otherwise(0))
    # Remove prev_travelled distance when rn == 0 (it happens when lag_end and begin overlap)
    dfs_state = dfs_state.withColumn('prev_travelled_distance', F.when(
        col('rn') == 0, 0).otherwise(col('prev_travelled_distance')))

    w = Window().partitionBy('userId').orderBy(
        'begin').rangeBetween(Window.unboundedPreceding, 0)

    dfs_state = dfs_state.withColumn('group', F.sum('rn').over(w))

    dfs_state = dfs_state.groupBy('userId', 'group').agg(F.mean('latitude').alias('latitude'),
                                                         F.mean('longitude').alias(
                                                             'longitude'),
                                                         F.min('begin').alias(
                                                             'begin'),
                                                         F.max('end').alias(
                                                             'end'),
                                                         F.first('state').alias('state')).drop('group')

    # Bug fix: due to the processing we do in the stop events, where we process stops every two days,
    # sometimes stop events overlap but they do not get merged until here. The error is RARE. Here we fix it
    #
    # We divide the two stops making MIN_STAY space between the two, if we can.
    w = Window().partitionBy('userId').orderBy('begin')
    dfs_state = dfs_state.withColumn('next_begin', F.lead('begin').over(w))
    dfs_state = dfs_state.withColumn('next_end', F.lead('end').over(w))
    dfs_state = dfs_state.withColumn('end', F.when(
        (col('next_begin').cast('long') - col('begin').cast('long') > 2 * MIN_STAY * 60) &
        (col('next_begin') < col('end')),
        col('next_begin') - F.expr("INTERVAL {} SECONDS".format(MIN_STAY * 60))
    ).otherwise(col('end')))
    dfs_state = dfs_state.drop('next_begin', 'next_end')

    dfs_destinations = get_destinations(dfs_state, roam_dist=roam_dist_stops)
    dfs_destinations = dfs_destinations.withColumn(
        'prefix', dfs_destinations.userId.substr(1, 2))
    dfs_destinations = dfs_destinations.withColumn(
        'dayofyear', F.dayofyear('begin'))
    dfs_destinations = dfs_destinations.withColumn('year', F.year('begin'))
    # dfs_destinations = dfs_destinations.withColumn('state', F.lit(state))

    # Local time
    dfs_destinations.createOrReplaceTempView("dfs_destinations")
    dfs_destinations = spark.sql("""
      SELECT dfs_destinations.*, geohash(clusterLatitude, clusterLongitude, 7) as geohash7
      from dfs_destinations
      """)
    dfs_destinations = dfs_destinations.withColumn(
        'geohash5', F.substring(col('geohash7'), 1, 5))
    dfs_destinations = dfs_destinations.join(
        F.broadcast(dfs_timezones), on='geohash5').drop('geohash5')
    dfs_destinations = dfs_destinations.withColumn(
        'local_begin', F.from_utc_timestamp(col('begin'), col('tzid')))
    dfs_destinations = dfs_destinations.withColumn('offset', (
        (col('local_begin').cast('long') - col('begin').cast('long')) / 3600).cast('int')).drop('local_begin')
    dfs_destinations.persist(StorageLevel.DISK_ONLY)

    # Write
    # output as country/prefix/part1..N
    local_dir_all = local_dir + "/all/"
    dfs_destinations_all = dfs_destinations.select(
        'prefix', 'userId', 'clusterId', 'begin', 'end', 'offset', 'year', 'dayofyear')
    dfs_destinations_all.repartition(8, 'dayofyear').write.format('parquet').mode(
        'overwrite').save(local_dir_all+"prefix="+prefix+"/")

    # output as country/prefix/state
    local_dir_distinct = local_dir+"/distinct/"
    dfs_destinations_distinct = dfs_destinations.select(
        'prefix', 'userId', 'clusterId', 'clusterLatitude', 'clusterLongitude', 'geohash7', 'state')
    dfs_destinations_distinct = dfs_destinations_distinct.drop_duplicates([
        'prefix', 'userId', 'clusterId', 'clusterLatitude', 'clusterLongitude', 'geohash7'])
    dfs_destinations_distinct.repartition("state").write.partitionBy(
        "state").format('parquet').mode('overwrite').save(local_dir_distinct+"prefix="+prefix+"/")

    dfs_destinations.unpersist()

    print("upload local data to azure")
    upload_time = time.time()

    # upload parts 1  "prefix/state"
    print(f"upload files for distinct")
    # upload with threads
    dfutures = []
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        fprefix = prefix
        print(f"upload files for distinct: {fprefix}")
        prefix_dir = local_dir_distinct+"prefix="+fprefix
        prefix_key = f"prefix={fprefix}"

        for state in US_STATES:
            s_key = f"state={state}"
            f_dir = prefix_dir + "/"+s_key
            f_key = prefix_key + "/"+s_key

            # print(f"read files for distinct from {f_dir}")

            if (os.path.isdir(f_dir)):
                files = [filename for filename in os.listdir(
                    f_dir) if filename.startswith("part-")]

                if len(files) > 0:

                    for file_local in files:
                        file_path = f_dir+"/"+file_local
                        part_num = int(file_local.split('-')[1])
                        part_key = '{:05d}'.format(part_num)
                        # fix name as static hash to be reproducible
                        filename_hash = hashlib.sha1(
                            str.encode(f_key+f_key+part_key)).hexdigest()

                        blob_key = "{}/{}/part-{}-{}.snappy.parquet".format(
                            path_out_distinct, f_key, part_key, filename_hash)

                        # print("upload " + file_path + " to " + container_out+":"+blob_key)
                        # upload_blob(blob_service_client,container_out, blob_key, file_path)
                        future = executor.submit(
                            upload_blob, blob_service_client, container_out, blob_key, file_path)
                        dfutures.append(future)

                # else:
                #    print(f"no files to upload for {f_key}")

            # else:
            #    print(f"missing partition for {f_key}")

        # end of loop, wait for futures
        for future in dfutures:
            bkey = future.result()

    # ensure we wait all tasks
    # TODO check if all done
    ddone = concurrent.futures.wait(dfutures)

    # upload parts 2 "prefix/parts"
    print(f"upload files for all")
    fprefix = prefix
    # upload with threads
    afutures = []
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        print(f"upload files for all: {fprefix}")
        prefix_dir = local_dir_all+"prefix="+fprefix
        prefix_key = f"prefix={fprefix}"

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

                    blob_key = "{}/{}/part-{}-{}.snappy.parquet".format(
                        path_out_all, prefix_key, part_key, filename_hash)

                    # print("upload " + file_path + " to " + container_out+":"+blob_key)
                    # upload_blob(blob_service_client,container_out, blob_key, file_path)
                    future = executor.submit(
                        upload_blob, blob_service_client, container_out, blob_key, file_path)
                    afutures.append(future)
            # else:
            #     print(f"no files to upload for {d_key}")

                # else:
                #     print(f"missing partition for {d_key}")
        # end of loop, wait for futures
        for future in afutures:
            bkey = future.result()

    # ensure we wait all tasks
    # TODO check if all done
    adone = concurrent.futures.wait(afutures)

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
