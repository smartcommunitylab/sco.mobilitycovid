package it.smartcommunitylab.sco.mobilitycovid.udf
import org.apache.spark.sql.api.java.UDF3
import com.github.davidmoten.geo._

class GeohashEncode extends UDF3[Double, Double, Integer, String] {
 def call(lat: Double, lon:Double, length:Integer): String = {
    GeoHash.encodeHash(lat,lon, length)
 }
}