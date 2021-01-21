name := "sco-mobilitycovid-udf"
version := "1.0"
organization := "it.smartcommunitylab"
scalaVersion := "2.11.12"
val sparkVersion = "3.1.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.0.1"
libraryDependencies += "com.github.davidmoten" % "geo" % "0.7.7"