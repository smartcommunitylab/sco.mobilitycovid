from __future__ import print_function

import argparse
import hashlib
import numpy as np
import os
import pandas as pd
import pygeohash as pgh
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

VERSION = 8

#
# Driver settings
#
SHUFFLE_PARTITIONS = 32
OUT_PARTITIONS = 2
CORES = "4"
RAM = "12g"
APP_NAME = "StopLocation"

# always set overwrite
WRITE_MODE = "overwrite"
SKIP_EXISTING = False


# templates
TABLE_PATH = "wasbs://{}@{}.blob.core.windows.net/{}/"
CONN_STRING = "BlobEndpoint=https://{}.blob.core.windows.net/;SharedAccessSignature={}"

# need leading slash
LOCAL_PATH = "./table/"


#
# Stop locations parameters
#
ROAM_DIST = 70  # meters
MIN_STAY = 5  # minutes
EARTH_RADIUS = 6372.795 * 1000
MAX_ACCURACY = 70
WINDOW_LENGTH = 1
NUM_OVERLAPPING_HOURS = 6

US_STATES = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS',
             'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']

# #
# # logging
# # set up logging to file - see previous section for more details
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
#                     datefmt='%m-%d %H:%M',
#                     filename='stops.log',
#                     filemode='w')
# # define a Handler which writes INFO messages or higher to the sys.stderr
# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# # set a format which is simpler for console use
# formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# # tell the handler to use this format
# console.setFormatter(formatter)
# # add the handler to the root logger
# logging.getLogger('').addHandler(console)


#
# Stop location lib
#


class PointsSet:
    """
    Class that stores a set of points in format (lat, lon)
    when the number of points > 3 the points are transformed into a set of vertices of convex hull. From this point
    on, the complexity stays, at the worst case, at O(n) for each added point
    """

    def __init__(self, points=None):
        """

        :param points: numpy array or list of points in format (lat, lon)
        """
        if points is None:
            points = []
        self._diameter = 0
        self.points = points[:]

    def add(self, point):
        """
        Add a point to the PointsSet. If the number of points is low, no ConvexHull is build, otherwise we build it
        and we add each point dynamically
        (https://www.geeksforgeeks.org/dynamic-convex-hull-adding-points-existing-convex-hull/)
        :param point: numpy array or list in format (lat, lon)
        """
        n = len(self.points)
        self.points = np.append(self.points, [point], axis=0)

    def diameter(self):
        """
        Computes the diameter of the PointsSet. The distance is the Haversine.
        If it is a convex hull, we use the rotating Calipers to find the diameter. We cache the diameter for future use.
        :return: float representing the diameter
        """
        if len(self.points) <= 1:
            return 0

        dist_mat = haversine(self.points)
        self._diameter = np.max(dist_mat)
        return self._diameter


def haversine(points, point2=None):
    """
    Haversine distance between a set of points
    :param points: numpy array or list of points in format (lat, lon)
    :return: float of the distance
    """
    if len(points.shape) == 1:
        points = points.reshape(-1, 2)
    try:
        lat, lng = points[:, 0], points[:, 1]
    except:
        raise Exception(points, point2)
    if point2 is None:
        idx1, idx2 = np.triu_indices(lat.size, 1)
        dflat = lat[idx2] - lat[idx1]
        dflng = lng[idx2] - lng[idx1]
        d = np.sin(dflat / 2) ** 2 + \
            np.cos(lat[idx2]) * np.cos(lat[idx1]) * np.sin(dflng / 2) ** 2
    else:
        dflat = lat - point2[0]
        dflng = lng - point2[1]
        d = np.sin(dflat / 2) ** 2 + \
            np.cos(point2[0]) * np.cos(lat) * np.sin(dflng / 2) ** 2

    return 2 * EARTH_RADIUS * np.arcsin(np.sqrt(d))


def get_medoid_index(dist_mat):
    """
    Given a vector of distances it returns the index of the medoid of those points.
    The medoid is the point closest to all others.
    :param dist_mat: vector of distances
    :return: index of the medoid.
    """
    return np.argmin(np.sum(dist_mat, axis=0))


def get_stop_location(df, roaming_distance, min_stay_duration, are_stops_daily_processed=True, use_medoid=False):
    """
    Given a numpy array of time-ordered gps locations detect the stop locations
    and return them in a pandas dataframe.
    Hariharan, Toyama. 2004. Project Lachesis: Parsing and Modeling Location
    Histories

    [[timestamp, latitude, longitude, t_start, t_end, personal, distance, geohash, after_stop_distance]]
    """
    # Inizialise variables
    i = 0
    stops_set = []

    if len(df) < 2:
        return stops_set

    df = np.array(df)

    # idxs = np.argsort(df[:, 0])
    # df = df[idxs]
    # Array containing timestamps
    time_array = df[:, 0]
    # Matrix containing latitude and longitude
    df_xyz = df[:, [1, 2]].astype(np.float64)
    # Array containing strings
    df_personal = df[:, -1]
    df_dists = df[:, -2].astype(np.float64)
    last_distance = 0
    next_day = datetime(
        time_array[0].year, time_array[0].month, time_array[0].day) + timedelta(days=1)
    while i < df.shape[0]:
        if (not are_stops_daily_processed) or (are_stops_daily_processed and time_array[i] < next_day):
            last_distance += df_dists[i]

        # Get the first item that is at least min_stay_duration away from
        # point i
        time_cutoff = time_array[i] + timedelta(minutes=min_stay_duration)
        idxs_after_time_cutoff = np.where(time_array >= time_cutoff)
        # Break out of while loop if there are no more items that satisfy the
        # time_cutoff criterium
        if len(idxs_after_time_cutoff[0]) == 0:
            break

        # This is the first item after that satisfies time_cutoff
        j_star = idxs_after_time_cutoff[0][0]
        if j_star < i:
            raise Exception(i, j_star, time_array[i], timedelta(
                minutes=min_stay_duration), time_array[i] + timedelta(minutes=min_stay_duration), time_array)

        # Check whether roaming_distance criterium is satisfied.
        pset = PointsSet(df_xyz[i:j_star + 1])
        if pset.diameter() > roaming_distance:
            i += 1
        else:
            # If the stop I should add is from the next day, I mark the previous stop with a distance 0.1
            # to avoid it gets merged.
            if are_stops_daily_processed and time_array[i] > next_day and stops_set:
                stops_set[-1][-1] += 0.1
                # I also do not add the stop
                last_distance = 0
                break

            for y in range(j_star + 1, df.shape[0]):
                pset.add(df_xyz[y])
                if pset.diameter() > roaming_distance:
                    break
                j_star = y

            if use_medoid:
                # Get medoid, if there are only 2 points just take the first one.
                if (j_star - i) == 1:
                    medoid_idx = 0
                else:
                    medoid_idx = get_medoid_index(
                        haversine(df_xyz[i:j_star + 1]))
                # Add medoid to list and increment i-index
                m = df_xyz[i + medoid_idx]
            else:
                m = np.mean(df_xyz[i:j_star + 1], axis=0)

            x = float(m[0])
            y = float(m[1])
            stops_set.append([len(stops_set), x, y, time_array[i], time_array[j_star], bool(np.any(
                df_personal[i:j_star + 1])), float(last_distance), pgh.encode(degrees(x), degrees(y), precision=6), 0])

            i = j_star + 1
            last_distance = 0

    if stops_set and are_stops_daily_processed:
        stops_set[-1][-1] += float(last_distance)

    # Convert to dataframe and return as result columns=["serial", timestamp", "latitude", "longitude", "t_start", "t_end", "personal", "distance", "geohash6", "after_stop_distance"]
    return stops_set

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
    requiredNamed.add_argument(
        "--geohashpath", type=str, help="Geohash file path")
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
    parser.add_argument("--prefix", type=str, default=None, help="User prefix")
    parser.add_argument("--accuracy", type=int,
                        default=MAX_ACCURACY, help="Max accuracy")
    parser.add_argument("--roam-dist", type=int,
                        default=ROAM_DIST, help="Roam dist")
    parser.add_argument("--min-stay", type=int,
                        default=MIN_STAY, help="Min stay")
    parser.add_argument("--overlap-hours", type=int,
                        default=NUM_OVERLAPPING_HOURS, help="Overlap hours next day")
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

    # Geohash file path
    geohash_path = args.geohashpath

    # Date, country, prefix
    country = args.country
    date_string = args.date
    prefix = args.prefix

    # Set date variables
    day_time = datetime.strptime(date_string, "%Y-%m-%d")
    year = day_time.year
    month = day_time.month
    day = day_time.day

    # stop config
    seconds = 60
    accuracy = args.accuracy
    roam_dist = args.roam_dist
    min_stay = args.min_stay
    overlap_hours = args.overlap_hours

    # Path in - path out
    blob_in = f"wasbs://{container_in}@{storage_account_name}.blob.core.windows.net/preprocessed/{country}/"
    path_out = f"stoplocation-v{VERSION}_r{roam_dist}-s{min_stay}-a{accuracy}-h{overlap_hours}/{country}"

    if prefix:
        path_out = f"stoplocation-v{VERSION}_prefix_r{roam_dist}-s{min_stay}-a{accuracy}-h{overlap_hours}/{country}"

    # config spark
    conf = getSparkConfig(cores, ram, shuffle_partitions, azure_accounts)

    # Create spark session
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession

    # Init azure client
    blob_service_client = BlobServiceClient.from_connection_string(
        CONN_STRING.format(storage_account_name, sas_token))

    #  build keys, date is mandatory, prefix opt
    partition_key = "year={}/month={}/day={}".format(year, month, day)
    if prefix:
        partition_key = "year={}/month={}/day={}/prefix={}".format(
            year, month, day, prefix)

    blob_base = "{}/{}".format(path_out, partition_key)

    #
    # check for skip
    # TODO
    #
    skip = False

    print("process "+partition_key + " to " + blob_base)
    start_time = time.time()
    local_dir = LOCAL_PATH+partition_key
    print("write temp to "+local_dir)

    # cleanup local if exists
    if (os.path.isdir(local_dir)):
        map(os.unlink, (os.path.join(local_dir, f)
                        for f in os.listdir(local_dir)))

    # TODO cleanup remote if exists

    # Output schema
    schema = ArrayType(StructType([
        #StructField('device_type', IntegerType(), False),
        StructField('serial', IntegerType(), False),
        StructField('latitude', DoubleType(), False),
        StructField('longitude', DoubleType(), False),
        StructField('begin', TimestampType(), False),
        StructField('end', TimestampType(), False),
        StructField('personal_area', BooleanType(), False),
        StructField('distance', DoubleType(), False),
        StructField('geohash6', StringType(), False),
        StructField('after_stop_distance', DoubleType(), False)
    ]))

    spark_get_stop_location = udf(
        lambda z: get_stop_location(z, roam_dist, min_stay), schema)

    # Geohash file
    print("read geohash parquet")
    csv_time = time.time()
    dfs_us_states = spark.read.format("parquet").load(geohash_path)
    # states = [s.STUSPS for s in dfs_us_states.select(
    #     'STUSPS').distinct().collect()]

    dfs_us_states = dfs_us_states.select(
        col('STUSPS').alias('state'), col('geohash').alias('geohash5'))
    dfs_us_states = dfs_us_states.drop_duplicates(subset=['geohash5'])

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

    # lit partition filters as data
    dfs_cur_partition = dfs_cur_partition.withColumn('year', F.lit(year))
    dfs_cur_partition = dfs_cur_partition.withColumn('month', F.lit(month))
    dfs_cur_partition = dfs_cur_partition.withColumn('day', F.lit(day))
    if prefix:
        dfs_cur_partition = dfs_cur_partition.withColumn(
            'prefix', F.lit(prefix))

    # read next day for overlap
    next_day = day_time + timedelta(days=1)
    next_partition_key = "year={}/month={}/day={}".format(
        next_day.year, next_day.month, next_day.day)
    if prefix:
        next_partition_key = "year={}/month={}/day={}/prefix={}".format(
            next_day.year, next_day.month, next_day.day, prefix)

    dfs_next_partition = spark.read.format(
        "parquet").load(f"{blob_in}/{next_partition_key}")
    dfs_next_partition = dfs_next_partition.where(
        F.hour("timestamp") <= (overlap_hours-1))

    # lit partition filters as data
    dfs_next_partition = dfs_next_partition.withColumn(
        'year', F.lit(next_day.year))
    dfs_next_partition = dfs_next_partition.withColumn(
        'month', F.lit(next_day.month))
    dfs_next_partition = dfs_next_partition.withColumn(
        'day', F.lit(next_day.day))
    if prefix:
        dfs_next_partition = dfs_next_partition.withColumn(
            'prefix', F.lit(prefix))

    # union with overlap
    dfs_partition = dfs_cur_partition.unionAll(dfs_next_partition)

    print("process with spark")
    spark_time = time.time()

    # select columns
    dfs_partition = dfs_partition.select('prefix', 'userID', 'timestamp', 'latitude', 'longitude',
                                         (F.when(col('opt1') == 'PERSONAL_AREA', True).otherwise(False)).alias('personal_area'), 'accuracy')

    # keep only data with required accuracy
    dfs_partition = dfs_partition.where(
        (col('accuracy') <= accuracy) & (col('accuracy') >= 0))

    # stats - enable only for debug!
    # num_inputs = dfs_partition.count()
    # print(f"read {num_inputs} rows from "+partition_key)

    # Lowering the granularity to 1 minutes

    # explicitely convert to timestamp
    #dfs_partition = dfs_partition.withColumn('timestamp', col('timestamp').cast('timestamp'))
    seconds_window = F.unix_timestamp(
        'timestamp') - F.unix_timestamp('timestamp') % seconds
    w = Window().partitionBy('userID', seconds_window).orderBy('accuracy')
    dfs_partition = dfs_partition.withColumn('rn', F.row_number().over(
        w).cast('int')).where(col('rn') == 1).drop('rn')

    # Radians lat/lon
    dfs_partition = dfs_partition.withColumn('latitude', F.radians(
        'latitude')).withColumn('longitude', F.radians('longitude'))

    # Groups GPS locations into chucks. A chunk is formed by groups of points that are distant no more than roam_dist
    w = Window.partitionBy(['prefix', 'userID']).orderBy('timestamp')
    dfs_partition = dfs_partition.withColumn(
        'next_lat', F.lead('latitude', 1).over(w))
    dfs_partition = dfs_partition.withColumn(
        'next_lon', F.lead('longitude', 1).over(w))

    # Haversine distance
    dfs_partition = dfs_partition.withColumn('distance_next', EARTH_RADIUS * 2 * F.asin(F.sqrt(
        F.pow(F.sin((col('next_lat') - col('latitude')) / 2.0), 2) + F.cos('latitude') * F.cos('next_lat') * F.pow(
            F.sin((col('next_lon') - col('longitude')) / 2.0), 2))))
    dfs_partition = dfs_partition.withColumn('distance_prev', F.lag(
        'distance_next', default=0).over(w))

    # Chunks
    dfs_partition = dfs_partition.withColumn('chunk', F.when(
        col('distance_prev') > roam_dist, 1).otherwise(0))

    windowval = (Window.partitionBy('prefix', 'userID').orderBy('timestamp')
                 .rangeBetween(Window.unboundedPreceding, 0))
    dfs_partition = dfs_partition.withColumn('chunk', F.sum(
        'chunk').over(windowval).cast('int'))

    # Remove chunks of the next day
    w = Window.partitionBy(['prefix', 'userID', 'chunk'])
    dfs_partition = dfs_partition.withColumn(
        'min_timestamp', F.dayofmonth(F.min('timestamp').over(w)))
    dfs_partition = dfs_partition.where(
        col('min_timestamp') == day).drop('min_timestamp')

    # Get the stops
    result_df = dfs_partition.groupBy('prefix', 'userID', 'chunk').agg(F.array_sort(F.collect_list(F.struct(
        'timestamp', 'latitude', 'longitude', 'distance_prev', 'personal_area'))).alias('gpsdata'), F.sum('distance_prev').alias('dist_sum'))
    result_df = result_df.withColumn(
        'gpsdata', spark_get_stop_location('gpsdata'))

    result_df = result_df.select(
        'userID', 'chunk', F.explode_outer('gpsdata').alias('e'), 'dist_sum')
    result_df = result_df.select('userID', 'chunk', col('e.latitude').alias('latitude'), col('e.longitude').alias('longitude'), col('e.begin').alias('begin'), col('e.end').alias(
        'end'), col('e.personal_area').alias('personal_area'), col('e.geohash6').alias('geohash6'), col('e.serial').alias('serial'), col('e.distance').alias('stop_distance'), col('e.after_stop_distance').alias('after_stop_distance'), 'dist_sum')
    result_df = result_df.fillna(0, subset=['after_stop_distance'])

    # Remove all those stop that start the next day
    result_df = result_df.where((col('begin').isNull()) | (
        F.dayofmonth('begin') != next_day.day))

    result_df = result_df.withColumn('isStop', F.when(
        col('serial').isNotNull(), 1).otherwise(0))

    result_df = result_df.withColumn('dist_sum', F.when(
        col('isStop') == 1, col('stop_distance')).otherwise(col('dist_sum')))

    windowval = (Window.partitionBy('userId').orderBy('chunk', 'serial')
                 .rowsBetween(Window.currentRow, Window.unboundedFollowing))
    result_df = result_df.withColumn(
        'isStop_cum', F.sum('isStop').over(windowval))

    result_df = result_df.groupBy('userId', 'isStop_cum').agg(F.first('latitude', ignorenulls=True).alias('latitude'),
                                                              F.first('longitude', ignorenulls=True).alias(
                                                                  'longitude'),
                                                              F.first('begin', ignorenulls=True).alias(
                                                                  'begin'),
                                                              F.first('end', ignorenulls=True).alias(
                                                                  'end'),
                                                              F.first('personal_area', ignorenulls=True).alias(
                                                                  'personal_area'),
                                                              F.first('geohash6', ignorenulls=True).alias(
                                                                  'geohash6'),
                                                              F.sum('dist_sum').alias(
                                                                  'prev_travelled_distance'),
                                                              F.sum('after_stop_distance').alias('after_stop_distance'))

    # compute next distance, which is null if it's the last
    windowval = Window.partitionBy('userId').orderBy(F.desc('isStop_cum'))
    result_df = result_df.withColumn('next_travelled_distance', F.lead(
        'prev_travelled_distance').over(windowval))
    result_df = result_df.withColumn('next_travelled_distance', F.when((col('next_travelled_distance').isNull()) & (
        col('after_stop_distance') > 0), col('after_stop_distance')).otherwise(col('next_travelled_distance')))

    # Drop nulls
    result_df = result_df.dropna(subset=['latitude']).drop('isStop_cum')

    # Transform latitude and longitude back to degrees
    result_df = result_df.withColumn('latitude', F.degrees('latitude'))
    result_df = result_df.withColumn('longitude', F.degrees('longitude'))

    # US states
    result_df = result_df.withColumn("geohash5", F.expr(
        "substring(geohash6, 1, length(geohash6)-1)"))
    result_df = result_df.join(F.broadcast(
        dfs_us_states), on="geohash5", how="inner").drop('geohash5')

    # lit partition data - enable only if added to partitionBy
    # result_df = result_df.withColumn('year', F.lit(year))
    # result_df = result_df.withColumn('month', F.lit(month))
    # result_df = result_df.withColumn('day', F.lit(day))

    # write
    out_partitions = len(US_STATES)
    result_df.repartition(out_partitions, "state").write.partitionBy("state").format(
        'parquet').mode("overwrite").save(local_dir+"/")

    # stats - enable only for debug!
    # num_records = result_df.count()
    # print(f"written {num_records} rows to "+local_dir)

    # if num_records == 0:
    #     raise Exception("Zero rows output")

    print("upload local data to azure")
    upload_time = time.time()

    # upload parts over states
    for state in US_STATES:
        print(f"upload files for {state}")
        state_dir = local_dir+"/state="+state
        state_key = f"{partition_key}/state={state}/"

        if (os.path.isdir(state_dir)):
            files = [filename for filename in os.listdir(
                state_dir) if filename.startswith("part-")]

            if len(files) > 0:

                for file_local in files:
                    file_path = state_dir+"/"+file_local
                    part_num = int(file_local.split('-')[1])
                    part_key = '{:05d}'.format(part_num)
                    # fix name as static hash to be reproducible
                    filename_hash = hashlib.sha1(
                        str.encode(state_key+part_key)).hexdigest()

                    blob_key = "{}/state={}/part-{}-{}.snappy.parquet".format(
                        blob_base, state, part_key, filename_hash)

                    print("upload " + file_path + " to " +
                          container_out+":"+blob_key)

                    blob_client = blob_service_client.get_blob_client(
                        container_out, blob_key)

                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)

                    # cleanup
                    os.remove(file_path)
            else:
                print(f"no files to upload for {state}")

        else:
            print(f"missing partition for {state}")

    print("--- {} seconds elapsed ---".format(int(time.time() - start_time)))
    print()
    stop_time = time.time()
    spark.stop()

    end_time = time.time()
    print("Done in {} seconds (csv:{} read:{} spark:{} upload:{} stop:{})".format(
        int(end_time - start_time),
        int(read_time - csv_time),
        int(spark_time - read_time),
        int(upload_time - spark_time),
        int(stop_time - upload_time),
        int(end_time - stop_time)
    ))
    print('Done.')
    #
    # END OF CODE
    #


if __name__ == "__main__":
    main()
