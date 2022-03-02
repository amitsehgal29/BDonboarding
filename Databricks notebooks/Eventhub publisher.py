# Databricks notebook source
df = spark.read.format("parquet").load("wasbs://files@expediademoblob.blob.core.windows.net/weather/weather/year=*/month=*/day=23/*.parquet")


# COMMAND ----------

from pyspark.sql.functions import to_json,col,struct
df = df.withColumn('body', to_json(struct([df[col] for col in df.columns]))).select('body')
connection_string="Endpoint=sb://expediademo.servicebus.windows.net/;SharedAccessKeyName=weatherdata;SharedAccessKey=86eXhjBuBqgAcwrFM065P+6ziwh61uwNdOPgbrTddYE=;EntityPath=weatherdata"
eventhub_conf = {}
eventhub_conf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
df.write.format('eventhubs').options(**eventhub_conf).save()

# COMMAND ----------


