# Spark UDF

Naive wrapper to expose as Spark UDF a geohash.encode Java function.
To build via `sbt` update the build file with dependencies and versions, and then run 

```
sbt clean package
```

to produce a deployable jar.

In order to run spark with the lib, add the jar as dependency via `--jars []` and then register the function.

For example

```
pyspark --jars sco-mobilitycovid-udf_2.11-1.0.jar

      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/

Using Python version 3.6.9 (default, Oct  8 2020 12:12:24)
SparkSession available as 'spark'.
>>> spark.udf.registerJavaFunction("geohash", "it.smartcommunitylab.sco.mobilitycovid.udf.GeohashEncode")
>>> 

```
