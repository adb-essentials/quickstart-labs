# Databricks notebook source
# MAGIC %md
# MAGIC ### STOP do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting Data to Cloud Storage
# MAGIC The following code downloads the Lakehouse Workshop testing data to the object storage paths set in the "00 - Set Lab Variables" notebook. If no variables are set, the code will use a default location in DBFS.

# COMMAND ----------

dbutils.widgets.text("UserDB", "")
dbutils.widgets.text("Data_PATH_Ingest", "")
dbutils.widgets.text("Data_PATH_User", "")

# COMMAND ----------

UserDB = dbutils.widgets.get("UserDB")
Data_PATH_Ingest = dbutils.widgets.get("Data_PATH_Ingest")
Data_PATH_User = dbutils.widgets.get("Data_PATH_User")

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
