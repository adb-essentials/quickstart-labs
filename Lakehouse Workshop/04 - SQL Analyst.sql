-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Lakehouse Labs for Data Analysts
-- MAGIC Welcome to the Lakehouse lab for data analysts on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Access your enterprise Lakehouse in Azure using Databricks SQL
-- MAGIC 2. Explore data sets using SQL powered by Photon
-- MAGIC
-- MAGIC ## The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

-- COMMAND ----------

-- MAGIC %run "../Lakehouse Workshop/00 - Set Lab Variables"

-- COMMAND ----------

-- MAGIC %run "../Lakehouse Workshop/04 - SQL Analyst Queries/04 - Create Queries"

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
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL1.1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open a new browser tab. In the lefthand navigation, change the persona switcher to SQL  
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL2.1.png" width="300">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Compute in Databricks SQL is called SQL Warehouses  
-- MAGIC SQL Warehouses are compute clusters optimized for DW and BI queries on the Lakehouse  
-- MAGIC They are powered by Delta and Photon - a C++ vectorized engine that is really fast  
-- MAGIC Photon is __*FREE*__ in Databricks SQL!  
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL3.1.png" width="150">
-- MAGIC
-- MAGIC SQL Warehouses come in 3 different [__*warehouse types*__](https://docs.databricks.com/sql/admin/warehouse-type.html) which allow you to mix and match compute to your target workload  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL4.2.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Databricks SQL Serverless is AWESOME!
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL4.3.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a SQL Warehouse  
-- MAGIC Navigate to SQL Warehouses and click Create SQL Warehouse
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL5.2.png" width="1200">
-- MAGIC
-- MAGIC Fill in the following fields:  
-- MAGIC **Name:** Lakehouse Labs  
-- MAGIC **Cluster size:** Small    
-- MAGIC **Auto stop:** On, 10 minutes (Serverless can be as low as 5 minutes via UI and 1 minute via API)  
-- MAGIC **Scaling:** Min 1 and Max 1   
-- MAGIC **Type:** Pro (or potentially Serverless if it is [turned on](https://learn.microsoft.com/en-us/azure/databricks/sql/admin/serverless))    
-- MAGIC **Advanced options Tags:** blank  
-- MAGIC **Advanced options Unity Catalog:** off  
-- MAGIC **Advanced options Spot instance policy:** Cost optimized  
-- MAGIC **Advanced options Channel:** Current  
-- MAGIC **Click Create**    
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL5.3.png" width="600">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse your databases and tables using Data Explorer
-- MAGIC In Data Explorer you can see the objects you have access to  
-- MAGIC For each table/view, you can view the size, schema, sample data, table details, and even version history of the table.  
-- MAGIC If you are using Unity Catalog, you'll also be able to view the table and column level lineage that is automatically captured by Databricks  
-- MAGIC If you are an admin, you can even manage permissions to the databases and tables in Data Explorer  
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL6.2.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The SQL Editor is a powerful SQL IDE built directly into Databricks SQL
-- MAGIC Browse and search for schema and table objects, view metadata, and explore tables/views  
-- MAGIC Use functionality like intellisense, view past query execution history, create data visualizations, and even download data  
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL7.2.png" width="500">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Queries are SQL code, results, and visualizations that you can save
-- MAGIC Create SQL quieres, save them, and share them with your team  
-- MAGIC View query results, or create data visualizations directly in the UI  
-- MAGIC Schedule refreshes of your queries so that the results are always up to date  
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL10.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 0, Create Your Databases and Tables
-- MAGIC Start this lab by executing query 0 which creates a Churn table using Copy Into and then performs performance optimizations using Optimize and Analyze, and finally loads the tables into to SQL Warehouse SSD cache.       
-- MAGIC Note that Databricks SQL is a DW engine that can be used for analysis, but also SQL based ETL routines like COPY INTO or CTAS (Create Table As Select)  
-- MAGIC The purpose of this lab is to focus on the SQL analysis capabilities
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL8.2.png" width="1200">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Analyze Our Data for Patterns and Metrics
-- MAGIC Data Analysts typically explore enterprise data for patterns, historical trends, and answering data-driven questions about the business. 
-- MAGIC
-- MAGIC ***Execute queries 1 through 9*** and attempt to answer the following questions:
-- MAGIC * Are our subscribers growing or shrinking? By how much every month?
-- MAGIC * What is the breakdown of *new* (recently subscribed) vs. *old* (subscribed 1 or more years ago) customers?
-- MAGIC * What is the renewal and cancellation rates among our subscribers? What % of user transactions are new subscriptions?
-- MAGIC * What is the distribution of payment amount among our subscribers? Does subscriber location impact the payment amount?
-- MAGIC * What is the distribution of listening rates (% of songs listened to) among our subscribers? Does gender impact the listening time?
-- MAGIC * Do subscribers who have been customers for a longer time use the platform more? Does it impact their likelihood to renew their subscription?
-- MAGIC
-- MAGIC All these questions can be answered with SQL against the data. Let's go through them one by one in Databricks SQL using the queries already created. 
