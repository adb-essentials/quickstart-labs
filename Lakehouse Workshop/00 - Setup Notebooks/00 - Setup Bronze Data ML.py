# Databricks notebook source
# MAGIC %md
# MAGIC STOP do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

Data_Path = "dbfs:/Lakehouse-Workshop/"
# abfss://<container>@<storage account>.dfs.core.windows.net/

# COMMAND ----------

User = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse(None)
User = User[:User.find("@")].replace(".","")

# COMMAND ----------

UserDB = User + "DB"

# COMMAND ----------

if Data_Path.endswith('/'):
  Data_PATH_Ingest = Data_Path[:-1] + "/" + "Labs_Ingest"
else: 
  Data_PATH_Ingest = Data_Path + "/" + "Labs_Ingest"

# COMMAND ----------

if Data_Path.endswith('/'):
  Data_PATH_User = Data_Path[:-1] + "/" + User
else: 
  Data_PATH_User = Data_Path + "/" + User

# COMMAND ----------

dbutils.fs.ls(Data_PATH_Ingest)

# COMMAND ----------

dbutils.fs.ls(Data_PATH_Ingest+'/members')

# COMMAND ----------

# delete the old database and tables if needed
_ = spark.sql("DROP DATABASE IF EXISTS {0} CASCADE".format(UserDB))

# drop any old delta lake files that might have been created
dbutils.fs.rm(Data_PATH_User + '/bronze', recurse=True)
dbutils.fs.rm(Data_PATH_User + '/gold', recurse=True)
dbutils.fs.rm(Data_PATH_User + '/silver', recurse=True)
dbutils.fs.rm(Data_PATH_User + '/checkpoint', recurse=True)

dbutils.fs.rm(Data_PATH_User + '/silver/member_feature', recurse=True)
dbutils.fs.rm(Data_PATH_User + '/silver/churndata', recurse=True)
dbutils.fs.rm(Data_PATH_User + '/silver/trainingdata', recurse=True)
dbutils.fs.rm(Data_PATH_User + '/gold/scoreddata', recurse=True)

# create database to house SQL tables
_ = spark.sql('CREATE DATABASE {0} LOCATION "{1}"'.format(UserDB, Data_PATH_User))

# COMMAND ----------

# DBTITLE 1,Set up necessary imports
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get Transaction Data
transaction_schema = StructType([
  StructField('msno', StringType()),
  StructField('payment_method_id', IntegerType()),
  StructField('payment_plan_days', IntegerType()),
  StructField('plan_list_price', IntegerType()),
  StructField('actual_amount_paid', IntegerType()),
  StructField('is_auto_renew', IntegerType()),
  StructField('transaction_date', DateType()),
  StructField('membership_expire_date', DateType()),
  StructField('is_cancel', IntegerType())  
  ])

# read data from parquet
transactions = (
  spark
    .read
    .csv(
      Data_PATH_Ingest+'/transactions_v2.csv',
      schema=transaction_schema,
      header=True,
      dateFormat='yyyyMMdd'
      )
    )

# persist in delta lake format
( transactions
    .write
    .format('delta')
    .partitionBy('transaction_date')
    .mode('overwrite')
    .save(Data_PATH_User + '/bronze/transactions')
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
    .save(Data_PATH_User + "/members")
  )

# COMMAND ----------

# DBTITLE 1,Get User Log Data
log_schema = StructType([
  StructField('msno', StringType()),
  StructField('date', StringType()),
  StructField('num_25', IntegerType()),
  StructField('num_50', StringType()),
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
    .save(Data_PATH_User + '/bronze/user_log/')
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
    .save(Data_PATH_User + '/bronze/train')
  )
