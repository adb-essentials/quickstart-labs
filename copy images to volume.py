# Databricks notebook source
import os
 
cwd = os.getcwd()

print(cwd)

# COMMAND ----------

# DBTITLE 1,Can I access the files from the repo?
dbutils.fs.ls(f"file:{os.getcwd()}/images")

# COMMAND ----------

# DBTITLE 1,Can I see the volume?
dbutils.fs.ls("/Volumes/lakehouselabs/default/images")

# COMMAND ----------

# DBTITLE 1,Copy images from repo to volume (bofa may need to use DBFS or something else... download images from GIT)
dbutils.fs.cp(f"file:{os.getcwd()}/images","/Volumes/lakehouselabs/default/images",True)

# COMMAND ----------

dbutils.fs.ls("/Volumes/lakehouselabs/default/images")

# COMMAND ----------

ajax-api/2.0/fs/files/Volumes/lakehouselabs/default/images/AutoML.png

# COMMAND ----------

# DBTITLE 1,Render an image in markdown cell
# MAGIC %md
# MAGIC <div><img width="1200px" src="ajax-api/2.0/fs/files/Volumes/lakehouselabs/default/images/AutoML.png"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC <div><img width="1200px" src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Lakehouse.png"/></div>

# COMMAND ----------

/Workspace/Repos/leo.furlong@databricks.com/quickstart-labs/images/AutoML.png

# COMMAND ----------

# MAGIC %sh cat /Volumes/lakehouselabs/default/images/AutoML.png

# COMMAND ----------

dbutils.fs.ls("abfss://volume@sscprivartifactadls.dfs.core.windows.net/boa/labs")
