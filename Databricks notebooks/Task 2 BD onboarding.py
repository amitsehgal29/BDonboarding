# Databricks notebook source
jdbcHostname = "bdonboarding.database.windows.net"
jdbcDatabase = "expedia_source"
jdbcusername = "bdadmin"
jdbcpassword = "demo@123"
jdbcport = "1433"
table = "raw.expedia"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcport, jdbcDatabase)


df = spark.read.format("jdbc")\
    .option("url",jdbcUrl)\
    .option("dbtable",table)\
    .option("user",jdbcusername)\
    .option("password",jdbcpassword)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()



# COMMAND ----------

df.write.format('delta').mode('overwrite').save("wasbs://expediaraw@expediademoblob.blob.core.windows.net/expedia")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.expedia USING DELTA LOCATION "wasbs://expediaraw@expediademoblob.blob.core.windows.net/expedia"

# COMMAND ----------

df = spark.read.format("jdbc")\
    .option("url",jdbcUrl)\
    .option("dbtable",table)\
    .option("user",jdbcusername)\
    .option("password",jdbcpassword)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

df.createOrReplaceTempView("incremental_expedia")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO raw.expedia
# MAGIC USING incremental_expedia
# MAGIC ON raw.expedia.id = incremental_expedia.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC id=incremental_expedia.id,
# MAGIC date_time=incremental_expedia.date_time,
# MAGIC site_name=incremental_expedia.site_name,
# MAGIC posa_continent=incremental_expedia.posa_continent,
# MAGIC user_location_country=incremental_expedia.user_location_country,
# MAGIC user_location_region=incremental_expedia.user_location_region,
# MAGIC user_location_city=incremental_expedia.user_location_city,
# MAGIC orig_destination_distance=incremental_expedia.orig_destination_distance,
# MAGIC user_id=incremental_expedia.user_id,
# MAGIC is_mobile=incremental_expedia.is_mobile,
# MAGIC is_package=incremental_expedia.is_package,
# MAGIC channel=incremental_expedia.channel,
# MAGIC srch_ci=incremental_expedia.srch_ci,
# MAGIC srch_co=incremental_expedia.srch_co,
# MAGIC srch_adults_cnt=incremental_expedia.srch_adults_cnt,
# MAGIC srch_children_cnt=incremental_expedia.srch_children_cnt,
# MAGIC srch_rm_cnt=incremental_expedia.srch_rm_cnt,
# MAGIC srch_destination_id=incremental_expedia.srch_destination_id,
# MAGIC srch_destination_type_id=incremental_expedia.srch_destination_type_id,
# MAGIC hotel_id=incremental_expedia.hotel_id
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC id,
# MAGIC date_time,
# MAGIC site_name,
# MAGIC posa_continent,
# MAGIC user_location_country,
# MAGIC user_location_region,
# MAGIC user_location_city,
# MAGIC orig_destination_distance,
# MAGIC user_id,
# MAGIC is_mobile,
# MAGIC is_package,
# MAGIC channel,
# MAGIC srch_ci,
# MAGIC srch_co,
# MAGIC srch_adults_cnt,
# MAGIC srch_children_cnt,
# MAGIC srch_rm_cnt,
# MAGIC srch_destination_id,
# MAGIC srch_destination_type_id,
# MAGIC hotel_id
# MAGIC   )
# MAGIC   VALUES (
# MAGIC incremental_expedia.id,
# MAGIC incremental_expedia.date_time,
# MAGIC incremental_expedia.site_name,
# MAGIC incremental_expedia.posa_continent,
# MAGIC incremental_expedia.user_location_country,
# MAGIC incremental_expedia.user_location_region,
# MAGIC incremental_expedia.user_location_city,
# MAGIC incremental_expedia.orig_destination_distance,
# MAGIC incremental_expedia.user_id,
# MAGIC incremental_expedia.is_mobile,
# MAGIC incremental_expedia.is_package,
# MAGIC incremental_expedia.channel,
# MAGIC incremental_expedia.srch_ci,
# MAGIC incremental_expedia.srch_co,
# MAGIC incremental_expedia.srch_adults_cnt,
# MAGIC incremental_expedia.srch_children_cnt,
# MAGIC incremental_expedia.srch_rm_cnt,
# MAGIC incremental_expedia.srch_destination_id,
# MAGIC incremental_expedia.srch_destination_type_id,
# MAGIC incremental_expedia.hotel_id
# MAGIC   )

# COMMAND ----------


