# Databricks notebook source
schemaTransactions = spark.read.parquet("/mnt/adbquickstart/transactions/").schema

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.create_table(
  comment="Parquet files from landing zone",
  table_properties={
    "quality": "bronze"
  }
)
def transactions_bronze():
  return (
    spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "parquet") \
      .schema(schemaTransactions) \
      .load("/mnt/adbquickstart/transactions/")
  )
