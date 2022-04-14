-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Quickstart for Data Analysts
-- MAGIC Welcome to the quickstart lab for data analysts on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Access your enterprise Lakehouse in Azure using Databricks SQL
-- MAGIC 2. Explore data sets using SQL powered by Photon
-- MAGIC 
-- MAGIC ## The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Initial Set-up
-- MAGIC 
-- MAGIC ### Enter the Blob_Container , Blob_Account and Account_Key for the Cloudlabs Environment

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("ACCOUNT_KEY", "", "ACCOUNT_KEY")
-- MAGIC dbutils.widgets.text("BLOB_CONTAINER", "", "BLOB_CONTAINER")
-- MAGIC dbutils.widgets.text("BLOB_ACCOUNT", "", "BLOB_ACCOUNT")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC BLOB_CONTAINER = dbutils.widgets.get("BLOB_CONTAINER")
-- MAGIC BLOB_ACCOUNT = dbutils.widgets.get("BLOB_ACCOUNT")
-- MAGIC ACCOUNT_KEY = dbutils.widgets.get("ACCOUNT_KEY")

-- COMMAND ----------

-- DBTITLE 1,Mount Blob Storage to DBFS
run = dbutils.notebook.run('./00 - Setup Storage', 60, {"BLOB_CONTAINER" : BLOB_CONTAINER,"BLOB_ACCOUNT" : BLOB_ACCOUNT,"ACCOUNT_KEY" : ACCOUNT_KEY })

-- COMMAND ----------

# delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS kkbox CASCADE')

# drop any old delta lake files that might have been created
dbutils.fs.rm('/mnt/adbquickstart/bronze', recurse=True)
dbutils.fs.rm('/mnt/adbquickstart/gold', recurse=True)
dbutils.fs.rm('/mnt/adbquickstart/silver', recurse=True)
dbutils.fs.rm('/mnt/adbquickstart/checkpoint', recurse=True)

-- COMMAND ----------

-- DBTITLE 1,Create Lab Queries
-- MAGIC %run "../ADBQuickStartLabs/00 - Create Queries"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore Your Data
-- MAGIC In 2018, [KKBox](https://www.kkbox.com/) - a popular music streaming service based in Taiwan - released a [dataset](https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data) consisting of a little over two years of (anonymized) customer transaction and activity data with the goal of challenging the Data & AI community to predict which customers would churn in a future period.  
-- MAGIC 
-- MAGIC The primary data files are organized in the storage container:
-- MAGIC 
-- MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/kkbox_filedownloads.png' width=150>
-- MAGIC 
-- MAGIC Read into dataframes, these files form the following data model:
-- MAGIC 
-- MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/kkbox_schema.png' width=150>
-- MAGIC 
-- MAGIC Each subscriber is uniquely identified by a value in the `msno` field of the `members` table. Data in the `transactions` and `user_logs` tables provide a record of subscription management and streaming activities, respectively.  

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Analyze Our Data for Patterns and Metrics
-- MAGIC Data Analysts typically explore enterprise data for patterns, historical trends, and answering data-driven questions about the business. 
-- MAGIC 
-- MAGIC In our scenario, we will attempt to answer the following questions:
-- MAGIC * Are our subscribers growing or shrinking? By how much every month?
-- MAGIC * What is the breakdown of *new* (recently subscribed) vs. *old* (subscribed 1 or more years ago) customers?
-- MAGIC * What is the renewal and cancellation rates among our subscribers? What % of user transactions are new subscriptions?
-- MAGIC * What is the distribution of payment amount among our subscribers? Does subscriber location impact the payment amount?
-- MAGIC * What is the distribution of listening rates (% of songs listened to) among our subscribers? Does gender impact the listening time?
-- MAGIC * Do subscribers who have been customers for a longer time use the platform more? Does it impact their likelihood to renew their subscription?
-- MAGIC 
-- MAGIC All these questions can be answered with SQL against the data. Let's go through them one by one in Databricks SQL

-- COMMAND ----------


