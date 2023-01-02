# Databricks notebook source
# MAGIC %md
# MAGIC ### Lakehouse Workshop Storage Variables
# MAGIC The following variable (**Data_Path**) will be used throughout the Lakehouse Workshop lab exercises to specify where data will be stored.  
# MAGIC If you do nothing, data will be ingested and stored in the Databricks File System (DBFS). While this is a bad practice for real usage of Databricks, it is fine for the Labs.  
# MAGIC If you'd like to attach to your own Azure Storage, please see the section **Attaching Storage Options** below.  
# MAGIC *Do not modify any other varables/code outside of Data_Path*

# COMMAND ----------

Data_Path = "dbfs:/Lakehouse-Workshop/"
# abfss://<container>@<storage account>.dfs.core.windows.net/

# COMMAND ----------

User = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse(None)
User = User[:User.find("@")].replace(".","")

# COMMAND ----------

UserDB = User + "DB"
print("UserDB: " + UserDB)

# COMMAND ----------

if Data_Path.endswith('/'):
  Data_PATH_Ingest = Data_Path[:-1] + "/" + "Labs_Ingest"
else: 
  Data_PATH_Ingest = Data_Path + "/" + "Labs_Ingest"
  
print("Data_PATH_Ingest: " + Data_PATH_Ingest)

# COMMAND ----------

if Data_Path.endswith('/'):
  Data_PATH_User = Data_Path[:-1] + "/" + User
else: 
  Data_PATH_User = Data_Path + "/" + User
  
print("Data_PATH_User: " + Data_PATH_User)

# COMMAND ----------

dbutils.fs.mkdirs(Data_PATH_Ingest)

# COMMAND ----------

dbutils.fs.mkdirs(Data_PATH_User)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attaching Storage Options
# MAGIC The following are options for interacting with your Azure Storage in the preferred order of best practices:  
# MAGIC 1. [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started)
# MAGIC 2. [Service Principal](https://learn.microsoft.com/en-us/azure/databricks/external-data/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal)
# MAGIC 3. [SAS Tokens](https://learn.microsoft.com/en-us/azure/databricks/external-data/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token)
# MAGIC 4. [Storage Mount](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts) **no longer recommended**
# MAGIC 5. [Credential Passthrough](https://learn.microsoft.com/en-us/azure/databricks/data-governance/credential-passthrough/adls-passthrough) **no longer recommended**
