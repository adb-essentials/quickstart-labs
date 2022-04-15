# Databricks notebook source
dbutils.widgets.text("ACCOUNT_KEY", "", "ACCOUNT_KEY")
dbutils.widgets.text("BLOB_CONTAINER", "", "BLOB_CONTAINER")
dbutils.widgets.text("BLOB_ACCOUNT", "", "BLOB_ACCOUNT")

# COMMAND ----------

BLOB_CONTAINER = dbutils.widgets.get("BLOB_CONTAINER")
BLOB_ACCOUNT = dbutils.widgets.get("BLOB_ACCOUNT")
ACCOUNT_KEY = dbutils.widgets.get("ACCOUNT_KEY")

# COMMAND ----------

print(BLOB_ACCOUNT)

# COMMAND ----------

# DBTITLE 1,Run this step only if you are re-running the notebook
try:
    dbutils.fs.unmount("/mnt/adbquickstart")
except:
  print("The storage isn't mounted so there is nothing to unmount.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting Azure Storage using an Access Key or Service Principal
# MAGIC We will mount an Azure blob storage container to the workspace using a shared Access Key. More instructions can be found [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage#--mount-azure-blob-storage-containers-to-dbfs). 
# MAGIC 
# MAGIC #####Note: For this Demo we are using access Key and mounting the blob on DBFS. Ideally one should authenticate using Service Principal and use full abfss path to access data

# COMMAND ----------

DIRECTORY = "/"
MOUNT_PATH = "/mnt/adbquickstart"

dbutils.fs.mount(
  source = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT}.blob.core.windows.net/KKBox-Dataset-orig/",
  mount_point = MOUNT_PATH,
  extra_configs = {
    f"fs.azure.account.key.{BLOB_ACCOUNT}.blob.core.windows.net":ACCOUNT_KEY
  }
)
