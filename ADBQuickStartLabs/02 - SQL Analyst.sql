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

-- MAGIC %py
-- MAGIC dbutils.widgets.text("ACCOUNT_KEY", "", "ACCOUNT_KEY")
-- MAGIC dbutils.widgets.text("BLOB_CONTAINER", "", "BLOB_CONTAINER")
-- MAGIC dbutils.widgets.text("BLOB_ACCOUNT", "", "BLOB_ACCOUNT")
-- MAGIC dbutils.widgets.text("Databricks_Token", "", "Databricks_Token")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Obtain a Personal Access Token and save it to the Databricks_Token widget  
-- MAGIC 1.Navigate to Settings -> User Settings  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT1.png" width="400">
-- MAGIC 
-- MAGIC 2.Under Access tokens -> Click Generate new token  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT2.png" width="300">
-- MAGIC 
-- MAGIC 3.Enter an optional description under comment -> Click Generate  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT3.png" width="400">
-- MAGIC 
-- MAGIC 4.Copy your token value to the clipboard -> Click Done  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT4.png" width="400">
-- MAGIC 
-- MAGIC 5.Save your token value to the Databricks_Token widget  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/PAT5.png" width="600">
-- MAGIC 
-- MAGIC You'll use the Databricks_Token again in the next lab

-- COMMAND ----------

-- MAGIC %py
-- MAGIC BLOB_CONTAINER = dbutils.widgets.get("BLOB_CONTAINER")
-- MAGIC BLOB_ACCOUNT = dbutils.widgets.get("BLOB_ACCOUNT")
-- MAGIC ACCOUNT_KEY = dbutils.widgets.get("ACCOUNT_KEY")
-- MAGIC Databricks_Token = dbutils.widgets.get("Databricks_Token")

-- COMMAND ----------

-- DBTITLE 1,Mount Blob Storage to DBFS
-- MAGIC %py
-- MAGIC run = dbutils.notebook.run('./Setup Notebooks/00 - Setup Storage', 60, {"BLOB_CONTAINER" : BLOB_CONTAINER,"BLOB_ACCOUNT" : BLOB_ACCOUNT,"ACCOUNT_KEY" : ACCOUNT_KEY })

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # delete the old database and tables if needed
-- MAGIC _ = spark.sql('DROP DATABASE IF EXISTS kkbox CASCADE')
-- MAGIC 
-- MAGIC # drop any old delta lake files that might have been created
-- MAGIC dbutils.fs.rm('/mnt/adbquickstart/bronze', recurse=True)
-- MAGIC dbutils.fs.rm('/mnt/adbquickstart/gold', recurse=True)
-- MAGIC dbutils.fs.rm('/mnt/adbquickstart/silver', recurse=True)
-- MAGIC dbutils.fs.rm('/mnt/adbquickstart/checkpoint', recurse=True)

-- COMMAND ----------

-- DBTITLE 1,Create Lab Queries
-- MAGIC %run "../ADBQuickStartLabs/Setup Notebooks/00 - Create Queries" $Databricks_Token = $Databricks_Token

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
-- MAGIC ### Databricks SQL is a DW and BI Engine directly on the Lakehouse  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open a new browser tab. In the lefthand navigation, change the persona switcher to SQL  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL2.png" width="300">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Compute in Databricks SQL is called SQL Endpoints  
-- MAGIC SQL Endpoints are compute clusters optimized for DW and BI queries on the Lakehouse  
-- MAGIC They are powered by Delta and Photon - a C++ vectorized engine that is really fast  
-- MAGIC Photon is __*FREE*__ in Databricks SQL!
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL3.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse your databases and tables using Data Explorer
-- MAGIC In Data Explorer you can see the objects you have access to  
-- MAGIC For each table/view, you can view the size, schema, sample data, table details, and even version history of the table  
-- MAGIC If you are an admin, you can even manage permissions to the databases and tables in Data Explorer  
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL4.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The SQL Editor is a powerful SQL IDE built directly into Databricks SQL
-- MAGIC Browse and search for schema and table objects, view metadata, and explore tables/views  
-- MAGIC Use functionality like intellisense, view past query execution history, create data visualization capabilities, and even download data  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL5.png" width="500">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Queries are SQL code, results, and visualizations that you can save
-- MAGIC Create SQL quieres, save them, and share them with your team  
-- MAGIC View query results, or create data visualizations directly in the UI  
-- MAGIC Schedule refreshes of your queries so that the results are always up to date  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL6.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 0, Create Your Databases and Tables
-- MAGIC If you didn't already complete the Data Engineering lab, you'll need to execute the Step 0 query  
-- MAGIC Step 0 creates the database and tables for the SQL Analyst lab using SQL ingestion techniques.  It also optimizes and analyzes those tables   
-- MAGIC Note that Databricks SQL is a DW engine that can be used for analysis, but also SQL based ETL routines like COPY INTO or CTAS (Create Table As Select)  
-- MAGIC The purpose of this lab is to focus on the SQL analysis capabilities
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL7.png" width="1200">

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
-- MAGIC All these questions can be answered with SQL against the data. Let's go through them one by one in Databricks SQL using the queries already created. 
