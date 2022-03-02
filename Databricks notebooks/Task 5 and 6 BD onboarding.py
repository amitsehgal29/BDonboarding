# Databricks notebook source
#Create Dataframes from raw tables in databricks

expedia_df = spark.sql("select * from raw.expedia")
hotel_df = spark.sql("select * from raw.hotel")
weather_df = spark.sql("select * from raw.weather")

# hotel_df.show()
# weather_df.show()



# COMMAND ----------

# Importing required functions from pyspark.sql
from pyspark.sql.functions import when,col,lit,datediff,lag,year,to_date,count,desc
from pyspark.sql import Window

# COMMAND ----------

# Condition to join hotels with weather table

condition = [(hotel_df['geohash'] == weather_df['geohash_weather']) \
             | (hotel_df['geohash'][1:4] == weather_df['geohash_weather'][1:4]) \
             | (hotel_df['geohash'][1:3] == weather_df['geohash_weather'][1:3])]

hotel_weather_precision_df = hotel_df.join(weather_df, condition, "inner")
# hotel_weather_precision_df.show(10,False)


# Adding column precision
hotel_weather_precision_df = hotel_weather_precision_df.withColumn('precision', \
                           when(col('geohash') == col('geohash_weather'), lit(5)) \
                           .when(col('geohash')[1:4] == col('geohash_weather')[1:4], lit(4)) \
                           .when(col('geohash')[1:3] == col('geohash_weather')[1:3], lit(3)))

# hotel_weather_precision_df.show(10,False)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create database if not exists publish

# COMMAND ----------



# segregation of valid and invalid bookings based on days 
expedia_idle_days = expedia_df.withColumn('idle_days', datediff(col('srch_ci'), lag(col('srch_ci'),1).over(Window.partitionBy(col('hotel_id')).orderBy(col('srch_ci')))))
expedia_idle_days = expedia_idle_days.fillna(0,("idle_days"))

valid_bookings = expedia_idle_days.filter(((col('idle_days') >=2) & (col('idle_days') <=30)))
# valid_bookings.show(10,False)


invalid_bookings = expedia_idle_days.subtract(valid_bookings)
# invalid_bookings.show(10,False)
invalid_bookings.coalesce(2).write.format('orc').mode('append').saveAsTable('publish.invalid_hotels')


expedia_hotel_df = valid_bookings.join(hotel_df, valid_bookings['hotel_id'] == hotel_df['id'], 'left')
# expedia_hotel_df.show(10,False)

bookings_by_country = expedia_hotel_df.groupBy('country').count()
# bookings_by_country.show()

bookings_by_city = expedia_hotel_df.groupBy('city').count()
# bookings_by_city.show()


bookings_by_country.coalesce(1).write.format('delta').mode('append').saveAsTable('publish.bookings_by_country')
bookings_by_city.coalesce(1).write.format('delta').mode('append').saveAsTable('publish.bookings_by_city')
valid_bookings_insert = valid_bookings.withColumn('year_srch_ci', year(to_date('srch_ci')))
valid_bookings_insert.coalesce(2).write.format('delta').mode('append').partitionBy('year_srch_ci').saveAsTable('publish.valid_hotels')

# COMMAND ----------

valid_bookings = valid_bookings.withColumnRenamed('id', 'booking_id')
combine_df = valid_bookings.join(hotel_weather_precision_df, valid_bookings['hotel_id'] == hotel_weather_precision_df['id'], 'left')


combine_avg_temp = combine_df.filter(col('avg_tmpr_c') > 0.0)
# combine_avg_temp.show(10,False)

stay_duration_df = combine_avg_temp.withColumn('stay_duration', datediff(col('srch_co'), col('srch_ci')))
# stay_duration_df.show(10,False)


preferred_stay_df = stay_duration_df.withColumn('cust_preference',                                                 when(((col('stay_duration').isNull()) | (col('stay_duration') <= 0) | (col('stay_duration') >= 30)), lit("Erroneous data")).when(col('stay_duration') == 1, lit('Short Stay')) 
                                               .when(((col('stay_duration') >= 1) | (col('stay_duration') <= 7)), lit('Standard Stay')) 
                                                    .when(((col('stay_duration') >= 8) | (col('stay_duration') <=14)), lit('Standard Extended Stay')) 
                                                    .when(((col('stay_duration') >14) | (col('stay_duration') <=28)), lit('Long Stay'))) 


preferred_stay_df1 = preferred_stay_df.groupBy("hotel_id", "cust_preference").agg(count("hotel_id").alias("Hotel_Max_Count")).sort(desc("Hotel_Max_Count")).limit(1)
preferred_stay_df1.coalesce(2).write.format('delta').mode('overwrite').saveAsTable('publish.cust_preference')


final_df = stay_duration_df.withColumn('with_children', when(col('srch_children_cnt') > 0, lit("Yes")).otherwise(lit("No")))
# final_df.show(10,False)
final_df.coalesce(2).write.format('delta').mode('overwrite').saveAsTable('publish.child_presence')


# COMMAND ----------

final_df.show()

# COMMAND ----------


