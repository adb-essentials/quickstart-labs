# Databricks notebook source
membersSchema = spark.read.parquet("/mnt/lakehouse/members/").schema
transactionsSchema = spark.read.parquet("/mnt/lakehouse/transactions/").schema
userlogsSchema = spark.read.parquet("/mnt/lakehouse/user_logs/").schema

# COMMAND ----------

# dbutils.fs.rm("/mnt/lakehouse/kkbox_bronze/members/",recurse=True)
# dbutils.fs.rm("/mnt/lakehosue/checkpoint/members",recurse=True)
# dbutils.fs.rm("/mnt/lakehouse/kkbox_bronze/transactions/",recurse=True)
# dbutils.fs.rm("/mnt/lakehosue/checkpoint/transactions",recurse=True)
# dbutils.fs.rm("/mnt/lakehouse/kkbox_bronze/user_logs/",recurse=True)
# dbutils.fs.rm("/mnt/lakehosue/checkpoint/user_logs",recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS kkbox_bronze

# COMMAND ----------

# "cloudFiles" indicates the use of Auto Loader

dfBronze_members = spark.readStream.format("cloudFiles") \
  .option('cloudFiles.format', 'parquet') \
  .schema(membersSchema) \
  .load("/mnt/lakehouse/members/")

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
dfBronze_members.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option('path', "/mnt/lakehouse/kkbox_bronze/members/") \
  .option("checkpointLocation", "/mnt/lakehosue/checkpoint/members") \
  .toTable("kkbox_bronze.members")

# COMMAND ----------

# "cloudFiles" indicates the use of Auto Loader

dfBronze_transactions = spark.readStream.format("cloudFiles") \
  .option('cloudFiles.format', 'parquet') \
  .schema(transactionsSchema) \
  .load("/mnt/lakehouse/transactions/")

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
dfBronze_transactions.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option('path', "/mnt/lakehouse/kkbox_bronze/transactions/") \
  .option("checkpointLocation", "/mnt/lakehosue/checkpoint/transactions") \
  .toTable("kkbox_bronze.transactions")

# COMMAND ----------

# "cloudFiles" indicates the use of Auto Loader

dfBronze_user_logs = spark.readStream.format("cloudFiles") \
  .option('cloudFiles.format', 'parquet') \
  .schema(userlogsSchema) \
  .load("/mnt/lakehouse/user_logs/")

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
dfBronze_user_logs.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option('path', "/mnt/lakehouse/kkbox_bronze/user_logs/") \
  .option("checkpointLocation", "/mnt/lakehosue/checkpoint/user_logs") \
  .toTable("kkbox_bronze.user_logs")
