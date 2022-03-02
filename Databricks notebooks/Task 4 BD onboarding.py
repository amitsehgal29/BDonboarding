# Databricks notebook source
import pygeohash as geohash
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

# COMMAND ----------

connection_string="Endpoint=sb://expediademo.servicebus.windows.net/;SharedAccessKeyName=weatherdata;SharedAccessKey=86eXhjBuBqgAcwrFM065P+6ziwh61uwNdOPgbrTddYE=;EntityPath=weatherdata"

import json 
start_event_position ={
    "offset": "-1",
    "seqNo": -1,
    "enqueuedTime": None,
    "isInclusive": True
}


eh_conf = {}
eh_conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
eh_conf["eventhubs.startingPosition"] = json.dumps(start_event_position)
weather_df = spark.read.format("eventhubs").options(**eh_conf).load()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType
from pyspark.sql.functions import from_json, col
weather_schema = StructType([
    StructField("lng", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("avg_tmpr_f", DoubleType()),
    StructField("avg_tmpr_c", DoubleType()),
    StructField("wthr_date", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
])

weather_df1 = weather_df.selectExpr("cast (body as string) as json").select(from_json("json", weather_schema).alias("data")).select("data.*")
# weather_df1.show(10,False)

# COMMAND ----------

weather_df2=weather_df1.withColumn("geohash_weather",geohash_udf(col("lat").cast("float"),col("lng").cast("float")))
# weather_df2.show(10,False)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS raw.weather
# MAGIC   (lng double,lat double, avg_tmpr_f double, avg_tmpr_c double, wthr_date string, year integer, month integer, day integer, geohash_weather string )
# MAGIC   USING delta
# MAGIC   LOCATION "wasbs://expediaraw@expediademoblob.blob.core.windows.net/weather"

# COMMAND ----------

weather_df2.write.mode("append").saveAsTable("raw.weather")

# COMMAND ----------


