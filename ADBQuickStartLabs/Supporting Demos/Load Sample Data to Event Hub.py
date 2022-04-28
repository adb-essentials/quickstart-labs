# Databricks notebook source
# Pyspark and ML Imports
import time, os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

EVENT_HUB_NAME = "ne-eventhubs.servicebus.windows.net"
CONSUMER_GROUP = "databricks"
EH_ENDPOINT = dbutils.secrets.get(scope = "ne-akv", key = "EH")
ehConf = { 
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EH_ENDPOINT),
  'ehName':EVENT_HUB_NAME,
  'eventhubs.consumerGroup':CONSUMER_GROUP,
  'maxEventsPerTrigger': 8000
}

# COMMAND ----------

# https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md

# COMMAND ----------

# Endpoint=sb://ne-eventhubs.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=k6S0qjLrax6X8DKqMqvOi9eWe0eWG+egtwp3/E3Rnos=;EntityPath=ne-eventhub

# COMMAND ----------

dbutils.fs.rm("/mnt/lakehouse/checkpoint/WriteToEventHub", recurse=True)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM kkbox.user_log--18,396,362

# COMMAND ----------

df = spark.readStream \
    .format("delta") \
    .option("maxFilesPerTrigger",1) \
    .load("dbfs:/mnt/adbquickstart/bronze/user_log") \
    .limit(40000)

# .table("kkbox.user_log")
# maxFilesPerTrigger
# maxBytesPerTrigger
# display(df)

# COMMAND ----------

# display(df)

# COMMAND ----------

# df.toJSON().selectExpr("value as body")

# COMMAND ----------

df = df.withColumn("body", to_json(struct(*[c for c in df.columns])))
# display(df)

# COMMAND ----------

ds = df \
  .select("body") \
  .writeStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .trigger(processingTime='10 seconds') \
  .option("checkpointLocation", "/mnt/lakehouse/checkpoint/WriteToEventHub") \
  .start()

# COMMAND ----------


