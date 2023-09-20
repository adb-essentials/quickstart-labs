# Databricks notebook source
# MAGIC %md
# MAGIC ### Lakehouse Workshop Storage Variables
# MAGIC The following variables will be used throughout the Lakehouse Workshop lab exercises to specify where data will be stored.  

# COMMAND ----------

LabCatalog = "lakehouselabs"
IngestionDB = "ingesteddata"
Data_PATH_Ingest = "/Volumes/{0}/{1}/kkbox_ingestion".format(LabCatalog, IngestionDB)


# COMMAND ----------

UserFull = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
User = UserFull[:UserFull.find("@")].replace(".","")

# COMMAND ----------

Databricks_Token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
Workspace = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

# COMMAND ----------

UserDB = User + "DB"
UserDB_DLT = User + "DB_DLT"
Data_PATH_User = "/Volumes/{0}/{1}/user_volume".format(LabCatalog, UserDB)
print("LabCatalog: " + LabCatalog)
print("IngestionDB: " + IngestionDB)
print("User: " + User)
print("UserDB: " + UserDB)
print("UserDB_DLT: " + UserDB_DLT)
print("Data_PATH_Ingest: " + Data_PATH_Ingest)
print("Data_PATH_User: " + Data_PATH_User)
print("Workspace: " + Workspace)
print("Databricks_Token has been set!")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS LakehouseLabs.{0}".format(UserDB))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS LakehouseLabs.{0}".format(UserDB_DLT))

# COMMAND ----------

spark.sql("""
          CREATE VOLUME IF NOT EXISTS {0}.{1}.user_volume;
          """.format(LabCatalog, UserDB))
