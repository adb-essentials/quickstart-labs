# Databricks notebook source
# MAGIC %md
# MAGIC STOP do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

# MAGIC %run "../00 - Set Lab Variables"

# COMMAND ----------

# DBTITLE 1,Set up necessary imports
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.sql(f"USE CATALOG {LabCatalog}")
spark.sql(f"USE {UserDB}")

# COMMAND ----------

# DBTITLE 1,Get Transaction Data
transactions = (
  spark.read.parquet('/Volumes/lakehouselabs/ingesteddata/kkbox_ingestion/transactions')
    )

# persist in delta lake format
( transactions
    .write
    .format('delta')
    .partitionBy('transaction_date')
    .mode('overwrite')
    .saveAsTable("bronze_transactions")
  )

# COMMAND ----------

# DBTITLE 1,Get Members Data
# members dataset schema
member_schema = StructType([
  StructField('msno', StringType()),
  StructField('city', IntegerType()),
  StructField('bd', IntegerType()),
  StructField('gender', StringType()),
  StructField('registered_via', IntegerType()),
  StructField('registration_init_time', StringType())
  ])

# read data from csv
members_csv = (
  spark
    .read
    .csv(
      Data_PATH_Ingest+'/members/members_v3.csv',
      schema=member_schema,
      header=True,
      dateFormat='yyyyMMdd'
      )
    )

# persist in delta lake format
(
  members_csv
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable("bronze_members")
  )

# COMMAND ----------

# MAGIC %sql DROP TABLE if EXISTS bronze_user_log

# COMMAND ----------

# DBTITLE 1,Get User Log Data
log_schema = StructType([
  StructField('msno', StringType()),
  StructField('date', StringType()),
  StructField('num_25', IntegerType()),
  StructField('num_50', IntegerType()),
  StructField('num_75', IntegerType()),
  StructField('num_985', IntegerType()),
  StructField('num_100', IntegerType()),
  StructField('num_unq', IntegerType()),
  StructField('total_secs', DoubleType())
  ])

# read data from csv
user_log_csv = (
  spark
    .read
    .csv(
      Data_PATH_Ingest+'/user_logs/user_logs_v2.csv',
      schema=log_schema,
      header=True,
      dateFormat='yyyyMMdd'
      )
    )

# persist in delta lake format
(
  user_log_csv
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('bronze_user_log')
  )

# COMMAND ----------

# DBTITLE 1,Get training (churn) dataset
# train dataset schema
train_schema = StructType([
  StructField('msno', StringType()),
  StructField('is_churn', IntegerType())
  ])

# read data from csv
train_csv = (
  spark
    .read
    .csv(
      Data_PATH_Ingest+'/train_v2.csv',
      schema=train_schema,
      header=True
      )
    )

# persist in delta lake format
(
  train_csv
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('bronze_train')
  )
