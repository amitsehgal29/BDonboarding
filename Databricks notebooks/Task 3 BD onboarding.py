# Databricks notebook source
file_location = "wasbs://files@expediademoblob.blob.core.windows.net/hotels/*.csv"
hotel_raw=spark.read.csv(file_location,inferSchema=True,header=True)

hotel_raw.createOrReplaceTempView("hotel_all_data")

hotel_invalid=spark.sql("select * from hotel_all_data where latitude is null or longitude is null or latitude rlike 'NA' or longitude rlike 'NA'")

# COMMAND ----------

from opencage.geocoder import OpenCageGeocode
import pygeohash as geohash

key = '58062003d54b49eb8bcd74b6355f09e4'
geocoder=OpenCageGeocode(key)


def get_lat_long(name, address, city, country, tag):
    value=geocoder.geocode('{},{},{},{}'.format(name,address,city,country))
    return (value[0]['geometry'] [tag])
udf_lat_long=udf(get_lat_long)


def geohashfunc(lat,long):
    if lat is None or long is None:
        lat=0.0
        long=0.0
    return(geohash.encode(lat,long,precision=5))
geohash_udf=udf(geohashfunc)


from pyspark.sql.functions import col,lit
hotel_mod = hotel_invalid.withColumn("g.lattitude",udf_lat_long(col("name"),col("address"),col("city"),col("country"),lit("lat"))).withColumn("g.longittude",udf_lat_long(col("name"),col("address"),col("city"),col("country"),lit("lng"))).drop('latitude','Longitude')
# display(hotel_mod)



hotel_valid=hotel_raw.subtract(hotel_invalid)
hotel_final=hotel_valid.union(hotel_mod)
hotel_gfinal=hotel_final.withColumn("geohash",geohash_udf(col("latitude").cast("float"),col("longitude").cast("float")))



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS raw.hotel
# MAGIC   (Id long,Name string, Country string, City string, Address string, Latitude string, Longitude string, geohash string)
# MAGIC   USING delta
# MAGIC   LOCATION "wasbs://expediaraw@expediademoblob.blob.core.windows.net/hotel"

# COMMAND ----------

hotel_gfinal.write.mode("append").saveAsTable("raw.hotel")
