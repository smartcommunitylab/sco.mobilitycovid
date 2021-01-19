import argparse
import os
import pandas as pd
import time

from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession


#
# Filters settings
#
ACCURACY = 300
INTERVAL_PRIOR = 30
INTERVAL_AFTER = 1

#
# Driver settings
#
SHUFFLE_PARTITIONS = 192
COALESCE_PARTITIONS = 16


APP_NAME = "processing"
WRITING_MODE = "overwrite"

#
# Argparser
#
def get_args():

    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Cuebiq data processor")
    parser.add_argument("--outstorage", type=str, help="Output storage")
    parser.add_argument("--outcontainer", type=str, help="Output container")
    parser.add_argument("--sas", type=str, help="SAS token")
    parser.add_argument("--acckey", type=str, help="Cuebiq secret key")
    parser.add_argument("--seckey", type=str, help="Cuebiq secret key")
    parser.add_argument("--state", type=str, help="State. Options: 'US','IT'")
    parser.add_argument("--date", type=str, help="Date. Format: 'YYYY-MM-DD'")
    parser.add_argument("--core", type=str, help="Number of cores")
    parser.add_argument("--ram", type=str, help="RAM quantity")
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
        
    # Date and country
        state = args.state
        cur_date = args.date
    
    # VM settings
    
        core = args.core
        ram = args.ram
        
    # S3 Cuebiq credentials
        cuebiq_access_key = args.acckey
        cuebiq_secret_key = args.seckey

    # Azure credentials
        sas_token = args.sas
        storage_account_name = args.outstorage
        container_name = args.outcontainer

    # Set date variables
        day_str = cur_date.replace("-","")
        day_time = datetime.strptime(cur_date,"%Y-%m-%d")
        start_date = day_time - timedelta(days=INTERVAL_PRIOR)
        end_date = day_time + timedelta(days=INTERVAL_AFTER)
    
    # Set S3 and Azure path
        cuebiq_container = "cuebiq-dataset-nv/"
        cuebiq_path = f"d4g/covid-19/{state}/"
        s3_day_path = f"s3a://{cuebiq_container}{cuebiq_path}{day_str}00/"
        output_path_azure = f"{state}/{day_str}/deltatable/"
        blob = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{output_path_azure}"
        
        
    # Setting enviroment variables and various drivers 
    # "org.apache.hadoop:hadoop-aws:2.7.3"      driver S3 (also 2.7.4 should work)
    # "org.apache.hadoop:hadoop-azure:2.10.0"   driver Azure
    # "io.delta:delta-core_2.12:0.7.0"          driver Delta-lake
    # "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"                        configuration Delta
    # "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"     configuration Delta
    # "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.AzureLogStore"         configuration Delta
        os.environ["PYSPARK_SUBMIT_ARGS"] = """--packages "org.apache.hadoop:hadoop-aws:2.7.3","org.apache.hadoop:hadoop-azure:2.10.0","io.delta:delta-core_2.12:0.7.0" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.AzureLogStore" pyspark-shell"""
        conf = (
        SparkConf()
        
            # Azure (Keys, Filesystem WASBS)
            .set("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .set(f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",sas_token)
            
            # S3 (Keys, Filesystem S3A)
            .set("spark.hadoop.fs.s3a.access.key", cuebiq_access_key)
            .set("spark.hadoop.fs.s3a.secret.key", cuebiq_secret_key)
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("com.amazonaws.services.s3.enableV4", True)
            .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            
            # SQL
            .set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
            .set("spark.sql.csv.filterPushdown.enabled","false")
            
            # Driver + memory
            .set("spark.driver.cores",core)
            .set("spark.shuffle.file.buffer","1m")
            .set("spark.memory.offHeap.enabled","true")
            .set("spark.memory.offHeap.size","3g")
            .set("spark.memory.fraction","0.8")
            .set("spark.memory.storageFraction","0.2")
            .set("spark.io.compression.lz4.blockSize","128k")
            .set("spark.driver.maxResultSize","0")
            .set("spark.driver.memory",ram)
            
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

        start_time = time.time()

        # Read data from S3
        print("Read csv from "+s3_day_path)
        ddf = spark.read.option("sep", "\t").csv(s3_day_path)
        count_line_input = ddf.count()
        
        # Register Spark DF as sql table
        table_name=f"cuebiq_{state}_{day_str}"
        ddf.registerTempTable(table_name)
        
        # Select and cast
        sdf = sqlContext.sql(f"select \
        cast(from_unixtime(_c0) as timestamp) as `timestamp`,\
        _c1 as `userId`,\
        cast(_c2 as tinyint) as `device`,\
        cast(_c3 as double) as `latitude`,\
        cast(_c4 as double) as `longitude`,\
        cast(_c5 as double) as `accuracy`,\
        cast(_c6 as int) as `offset`,\
        _c7 as `opt1`,\
        _c8 as `opt2`,\
        '{table_name}' as `source`,\
        year(from_unixtime(_c0)) as `year`,\
        month(from_unixtime(_c0)) as `month`,\
        dayofmonth(from_unixtime(_c0)) as `day`,\
        substr(_c1, 0, 2) as `prefix`\
        from {table_name} \
        where\
        _c5 <= {ACCURACY} AND\
        _c0 between {start_date.timestamp()} and {end_date.timestamp()}")

        # Reduce the number of partition for the output datasets 
        cdf = sdf.coalesce(COALESCE_PARTITIONS)
        
        count_line_output = cdf.count()
        
        # Write partitioned - append mode, replace all data with "overwrite", 
        cdf.write.partitionBy("year","month","day","prefix").format("delta").mode(WRITING_MODE).save(blob)
        
        end_time = time.time()
        
        cols = ["table_name","count_line_input","count_line_output","start_time","end_time",]
        vals = [[table_name,count_line_input,count_line_output,start_time,end_time]]
        pdf = pd.DataFrame(vals, columns=cols)
        pdf.to_csv(f"stats_{cur_date}_{state}.csv",index=False)
        
        
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. \nArguments:\n{ex}")

    finally:
        # Done and close spark session
        print("Done!")
        spark.stop()
  
if __name__ == "__main__":
    main()