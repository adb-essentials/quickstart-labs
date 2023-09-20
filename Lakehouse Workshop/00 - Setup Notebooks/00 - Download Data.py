# Databricks notebook source
# MAGIC %md
# MAGIC ### Run this notebook 1x for data ingestion setup for an entire team

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting Data to Cloud Storage
# MAGIC The following code downloads the Lakehouse Workshop testing data to the object storage paths set in the "00 - Set Lab Variables" notebook. If no variables are set, the code will use a default location in DBFS.

# COMMAND ----------

dbutils.widgets.text("LabCatalog", "")
dbutils.widgets.text("IngestionDB", "")
dbutils.widgets.text("Data_Path_Catalog", "")

# COMMAND ----------

LabCatalog = dbutils.widgets.get("LabCatalog")
IngestionDB = dbutils.widgets.get("IngestionDB")
Data_Path_Catalog = dbutils.widgets.get("Data_Path_Catalog")

# COMMAND ----------

spark.sql("""
          CREATE CATALOG IF NOT EXISTS {0}
          MANAGED LOCATION '{1}'
          COMMENT 'This is the Lakehouse Labs catalog';
          """.format(LabCatalog, Data_Path_Catalog))

# COMMAND ----------

spark.sql("""
          CREATE SCHEMA IF NOT EXISTS {0}.{1}
          COMMENT "This is the data ingestion schema";
          """.format(LabCatalog, IngestionDB))

# COMMAND ----------

spark.sql("""
          CREATE VOLUME IF NOT EXISTS {0}.{1}.kkbox_ingestion;
          """.format(LabCatalog, IngestionDB))

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

Data_PATH_Ingest = "/Volumes/{0}/{1}/kkbox_ingestion".format(LabCatalog, IngestionDB)

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

# COMMAND ----------

spark.sql("GRANT USE CATALOG, CREATE SCHEMA ON CATALOG {0} to `users`".format(LabCatalog))

# COMMAND ----------

spark.sql("GRANT READ VOLUME ON SCHEMA {0}.{1} to `users`".format(LabCatalog, IngestionDB))
