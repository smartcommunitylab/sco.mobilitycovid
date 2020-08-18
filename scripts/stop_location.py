from __future__ import print_function

import argparse
import hashlib
import numpy as np
import os
import pandas as pd
import pygeohash as pgh
import time

from datetime import date,datetime,timedelta
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


#
# Driver settings
#
SHUFFLE_PARTITIONS = 32
COALESCE_PARTITIONS = 8
CORE = "32"
RAM = "240g"
APP_NAME = "test"
# always set overwrite
WRITE_MODE = "overwrite"
SKIP_EXISTING = False


# templates
TABLE_PATH = "wasbs://{}@{}.blob.core.windows.net/{}/"
PARTITION = "year = {} and month = {} and day = {} and state = '{}' and prefix = '{}'"

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
        d = np.sin(dflat / 2) ** 2 + np.cos(lat[idx2]) * np.cos(lat[idx1]) * np.sin(dflng / 2) ** 2
    else:
        dflat = lat - point2[0]
        dflng = lng - point2[1]
        d = np.sin(dflat / 2) ** 2 + np.cos(point2[0]) * np.cos(lat) * np.sin(dflng / 2) ** 2

    return 2 * EARTH_RADIUS * np.arcsin(np.sqrt(d))


def get_medoid_index(dist_mat):
    """
    Given a vector of distances it returns the index of the medoid of those points.
    The medoid is the point closest to all others.
    :param dist_mat: vector of distances
    :return: index of the medoid.
    """
    return np.argmin(np.sum(dist_mat, axis=0))


def get_stop_location(df):
    """
    Given a numpy array of time-ordered gps locations detect the stop locations
    and return them in a pandas dataframe.
    Hariharan, Toyama. 2004. Project Lachesis: Parsing and Modeling Location
    Histories

    [[timestamp, latitude, longitude, t_start, t_end, personal, geohash]]
    """
    min_stay_duration = MIN_STAY
    roaming_distance = ROAM_DIST
    use_medoid=False
    # Inizialise variables
    i = 0
    stops_set = []

    df = np.array(df)

    #idxs = np.argsort(df[:, 0])
    #df = df[idxs]
    # Array containing timestamps
    time_array = df[:, 0]
    # Matrix containing latitude and longitude
    df_xyz = df[:, [1, 2]].astype(np.float64)
    # Array containing strings
    df_personal = df[:, -1]
    df_dists = df[:, -2].astype(np.float64)
    last_distance = 0
    while i < df.shape[0]:
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
          raise Exception(i, j_star, time_array[i], timedelta(minutes=min_stay_duration), time_array[i] + timedelta(minutes=min_stay_duration), time_array)

        # Check whether roaming_distance criterium is satisfied.
        pset = PointsSet(df_xyz[i:j_star + 1])
        if pset.diameter() > roaming_distance:
            i += 1
        else:
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
                    medoid_idx = get_medoid_index(haversine(df_xyz[i:j_star + 1]))
                # Add medoid to list and increment i-index
                m = df_xyz[i + medoid_idx]
            else:
                m = np.mean(df_xyz[i:j_star + 1], axis=0)

            x = float(m[0])
            y = float(m[1])
            stops_set.append([x, y, time_array[i], time_array[j_star], bool(np.any(df_personal[i:j_star + 1])), float(last_distance), pgh.encode(degrees(x), degrees(y), precision=6)])

            i = j_star + 1
            last_distance = 0

    # Convert to dataframe and return as result columns=["timestamp", "latitude", "longitude", "t_start", "t_end", "personal", "geohash5", "geohash6"]
    return stops_set


#
# Utils
#
def list_states(path,column):
    df = pd.read_csv(path)
    states = list(df[column].unique())
    for i in states:
        yield i


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(days=n)



#
# Argparser
#
def get_args():

    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Cuebiq data processor")
    parser.add_argument("--vmcore", default='2', type=str, help="Azure VM cores")
    parser.add_argument("--vmram", default='6g', type=str, help="Azure VM ram")
    parser.add_argument("--storage", type=str, help="Azure storage")
    parser.add_argument("--incontainer", type=str, help="Input container")
    parser.add_argument("--outcontainer", type=str, help="Output container")
    parser.add_argument("--sas", type=str, help="SAS token")
    parser.add_argument("--country", type=str, help="Country. Options: 'US','IT'")
    parser.add_argument("--date", type=str, help="Date. Format: 'YYYY-MM-DD'")
    parser.add_argument("--geohashpath", type=str, help="Geohash file path")
    parser.add_argument("--prefix", type=str, help="Userprefix")
    parsed_args = parser.parse_args()

    return parsed_args


#
# Main function
#
def main():

    """Main function"""

    try:

    # Get args
        args = get_args()

    # VM
        if 'vmcore' in args:
            CORE = args.vmcore

        if 'vmram' in args:
            RAM = args.vmram

    # Date, country, prefix
        country = args.country
        date_string = args.date
        prefix = args.prefix

    # Azure credentials
        sas_token = args.sas
        storage_account_name = args.storage
        container_in = args.incontainer
        container_out = args.outcontainer

    # Geohash file path
        geohash_path = args.geohashpath

    # Set date variables
        date_partition = date_string.replace("-","")
        day_time = datetime.strptime(date_string,"%Y-%m-%d")
        year = str(day_time.year)
        month = str(day_time.month)
        day = str(day_time.day)

    # Path in - path out
        #blob_in = f"wasbs://{container_in}@{storage_account_name}.blob.core.windows.net/{country}/{date_partition}/deltatable/year={year}/month={month}/day={day}/"

        blob_in = f"wasbs://{container_in}@{storage_account_name}.blob.core.windows.net/preprocessed/{country}/"
        path_out = f"stoplocation/{country}"

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

            # Azure (Keys, Filesystem WASBS)
            .set("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .set(f"fs.azure.sas.{container_in}.{storage_account_name}.blob.core.windows.net",sas_token)
            .set(f"fs.azure.sas.{container_out}.{storage_account_name}.blob.core.windows.net",sas_token)

            # SQL
            .set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
            .set("spark.sql.csv.filterPushdown.enabled","false")

            # Driver + memory
            .set("spark.driver.cores",CORE)
            .set("spark.shuffle.file.buffer","1m")
            # .set("spark.memory.offHeap.enabled","true")
            # .set("spark.memory.offHeap.size","3g")
            .set("spark.memory.fraction","0.8")
            .set("spark.memory.storageFraction","0.2")
            .set("spark.io.compression.lz4.blockSize","128k")
            .set("spark.driver.maxResultSize","0")
            .set("spark.driver.memory",RAM)

            # Local storage for spilling & storing temp files
            .set("spark.local.dir","/mnt/batch/tasks/shared")

            # Set master local
            .setMaster("local[*]")

            # App name
            .setAppName(APP_NAME)
        )

        # Create spark session
        sc = SparkContext(conf=conf).getOrCreate()
        sqlContext = SQLContext(sc)
        spark=sqlContext.sparkSession


        # Init azure client
        blob_service_client = BlobServiceClient.from_connection_string(
            CONN_STRING.format(storage_account_name, sas_token))

    #
    # CODE STARTS HERE
    #


        # Geohash file
        dfs_us_states = spark.read.format("csv").option("header", True).load(geohash_path)
        dfs_us_states = dfs_us_states.select(col('STUSPS').alias('state'), col('geohash').alias('geohash5'))
        dfs_us_states = dfs_us_states.drop_duplicates(subset=['geohash5'])

        # Output schema
        schema = ArrayType(StructType([
                    #StructField('device_type', IntegerType(), False),
                    StructField('lat', DoubleType(), False),
                    StructField('lon', DoubleType(), False),
                    StructField('begin', TimestampType(), False),
                    StructField('end', TimestampType(), False),
                    StructField('personal_area', BooleanType(), False),
                    StructField('distance', DoubleType(), False),
                    StructField('geohash6', StringType(), False)
            ]))

        spark_get_stop_location = udf(lambda z: get_stop_location(z), schema)


        # Uploading
        for state in list_states(geohash_path,"STUSPS"):
            partition_key = PARTITION.format(
                year, month, day, state)

            partition_path = "prefix={}/year={}/month={}/day={}/state={}/".format(
                prefix, year, month, day, state)

            dest_path = "{}/{}".format(path_out, partition_path)

            # check if dest exists to skip
            skip = False

            if(SKIP_EXISTING):
                try:
                    dest_client = blob_service_client.get_blob_client(
                        container_out, dest_path)
                    # One of the very few methods which do not mutate state
                    properties = dest_client.get_blob_properties()
                    if 'hdi_isfolder' in properties.metadata:
                        # skip
                        skip = True
                except ResourceNotFoundError:
                    # Not found
                    skip = False
                    pass

            if skip:
                print("skip "+partition_key + " to " + dest_path)
            else:
                print("process "+partition_key + " to " + dest_path)
                local_dir = LOCAL_PATH+partition_path
                
                # cleanup local if exists
                if (os.path.isdir(local_dir)):
                    map(os.unlink, (os.path.join(local_dir, f)
                                    for f in os.listdir(local_dir)))

                # TODO cleanup remote if exists
                
                
                print('processing day:', day)          
                print(f"processing prefix {prefix}")
                
                # Input dataset
                dfs = spark.read.format("parquet").load(blob_in)
                dfs_day = dfs.where(f"(day == {day} AND month == {month} AND year == {year})")
                dfs_day_prefix = dfs_day.where(f"(prefix == '{prefix}')")
                dfs_day_prefix = dfs_day_prefix.select('prefix',col('userID').alias('ID'), 'timestamp', col('latitude').alias('lat'), col('longitude').alias("lon"),
                        (F.when(col('opt1') == 'PERSONAL_AREA', True).otherwise(False)).alias('personal_area'), 'accuracy')

                # keep only data with required accuracy
                
                dfs_day_prefix = dfs_day_prefix.where((col('accuracy') <= MAX_ACCURACY) & (col('accuracy') >= 0))

                start_time = time.time()
                    
                # Lowering the granularity to 1 minutes
                seconds = 60
                seconds_window = F.from_unixtime(F.unix_timestamp('timestamp') - F.unix_timestamp('timestamp') % seconds)
                dfs_day_prefix = dfs_day_prefix.withColumn('timestamp', col('timestamp').cast('timestamp'))
                dfs_day_prefix = dfs_day_prefix.withColumn('timestamp', seconds_window.cast('timestamp'))
                w = Window().partitionBy('ID', 'timestamp').orderBy('accuracy')
                dfs_day_prefix = dfs_day_prefix.withColumn('rn', F.row_number().over(w)).where(col('rn') == 1).drop('rn')

                # Radians lat/lon
                dfs_day_prefix = dfs_day_prefix.withColumn('lat', F.radians('lat')).withColumn('lon', F.radians('lon'))

                # Groups GPS locations into chucks. A chunk is formed by groups of points that are distant no more than roam_dist
                w = Window.partitionBy(['prefix', 'ID']).orderBy('timestamp')
                dfs_day_prefix = dfs_day_prefix.withColumn('next_lat', F.lead('lat', 1).over(w))
                dfs_day_prefix = dfs_day_prefix.withColumn('next_lon', F.lead('lon', 1).over(w))

                # Haversine distance
                dfs_day_prefix = dfs_day_prefix.withColumn('distance_next', EARTH_RADIUS * 2 * F.asin(F.sqrt(
                            F.pow(F.sin((col('next_lat') - col('lat')) / 2.0), 2) + F.cos('lat') * F.cos('next_lat') * F.pow(
                                F.sin((col('next_lon') - col('lon')) / 2.0), 2))))
                dfs_day_prefix = dfs_day_prefix.withColumn('distance_prev', F.lag('distance_next', default=0).over(w))
                dfs_day_prefix = dfs_day_prefix.withColumn('chunk', F.when(col('distance_prev') > ROAM_DIST, 1).otherwise(0))

                windowval = (Window.partitionBy('prefix', 'ID').orderBy('timestamp')
                                    .rangeBetween(Window.unboundedPreceding, 0))
                dfs_day_prefix = dfs_day_prefix.withColumn('chunk', F.sum('chunk').over(windowval))

                # Get the stops
                result_df = dfs_day_prefix.groupBy('prefix', 'ID', 'chunk').agg(F.array_sort(F.collect_list(F.struct('timestamp', 'lat', 'lon', 'distance_prev', 'personal_area'))).alias('gpsdata'), F.sum('distance_prev').alias('dist_sum'))
                result_df = result_df.withColumn('gpsdata', spark_get_stop_location('gpsdata'))

                # Computations to check the travelled distance within the day
                result_df = result_df.withColumn('isStop', F.when(F.size('gpsdata') > 0, 1).otherwise(0))
                windowval = (Window.partitionBy('prefix', 'ID').orderBy('chunk')
                                    .rowsBetween(Window.unboundedPreceding, Window.currentRow))
                result_df = result_df.withColumn('isStop_cum', F.sum('isStop').over(windowval))
                result_df = result_df.withColumn('dist_sum', F.when(col('isStop') == 1, 0).otherwise(col('dist_sum')))
                windowval = (Window.partitionBy('prefix', 'ID').orderBy('isStop_cum').rangeBetween(0, 0))
                result_df = result_df.withColumn('next_travelled_distance', F.sum('dist_sum').over(windowval))
                windowval = (Window.partitionBy('prefix', 'ID').orderBy('isStop_cum').rangeBetween(-1, -1))
                result_df = result_df.withColumn('prev_travelled_distance', F.sum('dist_sum').over(windowval))
                # Exclude non stops
                result_df = result_df.where('isStop = 1')

                result_df = result_df.select('ID', 'chunk', F.explode('gpsdata').alias('e'), 'prev_travelled_distance', 'next_travelled_distance')
                result_df = result_df.select(col('ID').alias('userId'), 'chunk', col('e.lat').alias('latitude'), col('e.lon').alias('longitude'), col('e.begin').alias('begin'), col('e.end').alias('end'), col('e.personal_area').alias('personal_area'), col('e.geohash6').alias('geohash6'), col('e.distance').alias('stop_distance'), 'next_travelled_distance', 'prev_travelled_distance')
                #windowval = Window.partitionBy('userId').orderBy('chunk')
                windowval = Window.partitionBy('userId').orderBy('begin')
                result_df = result_df.withColumn('next_stop_distance', F.lead('stop_distance', default=0).over(windowval))
                result_df = result_df.withColumn('prev_travelled_distance', (col('stop_distance')+col('prev_travelled_distance')).cast('int'))
                result_df = result_df.withColumn('next_travelled_distance', (col('next_stop_distance')+col('next_travelled_distance')).cast('int'))
                result_df = result_df.drop('stop_distance').drop('next_stop_distance')

                result_df = result_df.withColumn('latitude', F.degrees('latitude'))
                result_df = result_df.withColumn('longitude', F.degrees('longitude'))

                # US states
                result_df = result_df.withColumn("geohash5", F.expr("substring(geohash6, 1, length(geohash6)-1)"))
                result_df = result_df.join(F.broadcast(dfs_us_states), on="geohash5", how="inner").drop('geohash5')

                result_df = result_df.withColumn('year', F.lit(day_time.year))
                result_df = result_df.withColumn('month', F.lit(day_time.month))
                result_df = result_df.withColumn('day', F.lit(day_time.day))
                result_df.repartition(COALESCE_PARTITIONS).write.partitionBy(
                            "year", "month", "day", "state").format('parquet').mode(WRITE_MODE).save(LOCAL_PATH+prefix+"/")
                

                # upload parts
                files = [filename for filename in os.listdir(
                    local_dir) if filename.startswith("part-")]

                if len(files) > 0:

                    for file_local in files:
                        file_path = local_dir+file_local
                        part_num = int(file_local.split('-')[1])
                        part_key = '{:05d}'.format(part_num)
                        # fix name as static hash to be reproducible
                        filename_hash = hashlib.sha1(
                            str.encode(partition_path+part_key)).hexdigest()

                        blob_key = "{}part-{}-{}.snappy.parquet".format(
                            dest_path, part_key, filename_hash)

                        print("upload " + file_path + " to " +
                            container_out+":"+blob_key)

                        blob_client = blob_service_client.get_blob_client(
                            container_out, blob_key)

                        with open(file_path, "rb") as data:
                            blob_client.upload_blob(data, overwrite=True)

                        # cleanup
                        os.remove(file_path)

                else:
                    print("error, no files to upload.")
                    pass
        

        print("--- {} seconds elapsed ---".format(int(time.time() - start_time)))
        print()
        print('Done.')
    #
    # END OF CODE
    #

    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. \nArguments:\n{ex}")
        pass

    finally:
        # Done and close spark session
        print("Done!")
        spark.stop()


if __name__ == "__main__":
    main()