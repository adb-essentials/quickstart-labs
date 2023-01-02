# Databricks notebook source
# MAGIC %md
# MAGIC ### STOP do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting Azure Storage using an Access Key or Service Principal
# MAGIC We will mount an Azure blob storage container to the workspace using a shared Access Key. More instructions can be found [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage#--mount-azure-blob-storage-containers-to-dbfs). 
# MAGIC 
# MAGIC #####Note: For this Demo we are using access Key and mounting the blob on DBFS. Ideally one should authenticate using Service Principal and use abfss driver and full path to access data

# COMMAND ----------

# MAGIC %run "/Repos/leo.furlong@databricks.com/Lakehouse-Workshop/Lakehouse Workshop/Setup Notebooks/00 - Lab Variables"

# COMMAND ----------

# TBD....save lab ingest files to GitHub Repo or Azure Blob Public?

# COMMAND ----------

# MAGIC %sh
# MAGIC # Pull CSV file from url
# MAGIC cd /dbfs/FileStore/
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/transactions_v2.csv
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/train_v2.csv
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/members/members_v3.csv
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/user_logs/user_logs_v2.csv
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/transactions/part-00000-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-0-1-c000.snappy.parquet
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/transactions/part-00001-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-1-1-c000.snappy.parquet
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/transactions/part-00002-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-2-1-c000.snappy.parquet
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/kkbox/KKBox-Dataset-orig/transactions/part-00003-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-3-1-c000.snappy.parquet

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore
# MAGIC ls

# COMMAND ----------

dbutils.fs.mkdirs(Data_PATH_Ingest)

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/transactions_v2.csv", Data_PATH_Ingest + "/" + "transactions_v2.csv")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/train_v2.csv", Data_PATH_Ingest + "/" + "train_v2.csv")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/members_v3.csv", Data_PATH_Ingest + "/members/" + "members_v3.csv")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/user_logs_v2.csv", Data_PATH_Ingest + "/user_logs/" + "user_logs_v2.csv")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/part-00000-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-0-1-c000.snappy.parquet", Data_PATH_Ingest + "/transactions/" + "part-00000-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-0-1-c000.snappy.parquet")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/part-00001-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-1-1-c000.snappy.parquet", Data_PATH_Ingest + "/transactions/" + "part-00001-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-1-1-c000.snappy.parquet")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/part-00002-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-2-1-c000.snappy.parquet", Data_PATH_Ingest + "/transactions/" + "part-00002-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-2-1-c000.snappy.parquet")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/part-00003-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-3-1-c000.snappy.parquet", Data_PATH_Ingest + "/transactions/" + "part-00003-tid-5999996899273686881-0be686b1-961c-40c0-a95f-db3350c2865c-3-1-c000.snappy.parquet")

# COMMAND ----------

dbutils.fs.ls(Data_PATH_Ingest)
