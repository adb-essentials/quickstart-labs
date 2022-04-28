# Databricks notebook source
from datetime import datetime as dt
import json

# Start from beginning of stream
startOffset = "-1"

# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

# COMMAND ----------

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
  'eventhubs.startingPosition': json.dumps(startingEventPosition),
  'maxEventsPerTrigger': 8000
}

# COMMAND ----------

dbutils.fs.rm("/mnt/lakehouse/checkpoint/WriteToDelta", recurse=True)

# COMMAND ----------

# https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md

# COMMAND ----------

# Endpoint=sb://ne-eventhubs.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=k6S0qjLrax6X8DKqMqvOi9eWe0eWG+egtwp3/E3Rnos=;EntityPath=ne-eventhub

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

df = spark.readStream.format("eventhubs") \
    .options(**ehConf) \
    .load()
# display(df)

# COMMAND ----------

# Schema of incoming data from IoT hub
schema = "msno STRING, date STRING, num_25 STRING, num_50 STRING, num_75 STRING, num_985 STRING, num_100 STRING, num_unq STRING, total_secs STRING"

# Read directly from IoT Hub using the EventHubs library for Databricks
stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('sequenceNumber','reading.*', F.to_date('enqueuedTime').alias('stream_date'))               # Create a "date" field for partitioning
)

# COMMAND ----------

stream.writeStream \
    .format("delta") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/mnt/lakehouse/checkpoint/WriteToDelta") \
    .outputMode("append") \
    .option('path', '/mnt/lakehouse/bronze/transactions_streaming/') \
    .toTable("kkbox.transactions_streaming")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE kkbox.transactions_streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM kkbox.transactions_streaming
