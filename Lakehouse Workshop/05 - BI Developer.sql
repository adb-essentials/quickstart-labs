-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Quickstart for Data Engineers on Delta Live Tables
-- MAGIC Welcome to the quickstart lab for data engineers on Azure Databricks for Delta Live Tables (DLT)! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Create a DLT notebook with Data Quality rules
-- MAGIC 2. Create a DLT Pipelines and run it
-- MAGIC 3. Connect Power BI to your DLT database using Databrick SQL
-- MAGIC 
-- MAGIC ## The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is create a DLT pipeline which will load a star schema in the Lakehouse to be used with Power BI and other BI tools through Databricks SQL. 
-- MAGIC 
-- MAGIC ***This DLT pipeline will not work unless you have already mounted your storage account. Refer to notebook "01 - Data Engineer" for mounting instructions.***

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Connect to the DLT Database using Databricks SQL and Power BI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Maintain Star Schemas for Power BI with Delta Live Tables
-- MAGIC To get the optimal performance from Power BI it is recommended to use a star schema data model and to make use of user-defined aggregated tables. However, as you build out your facts, dimensions, and aggregation tables and views in Delta Lake, ready to be used by the Power BI data model, it can become complex to manage all the pipelines, dependencies, and data quality.  
-- MAGIC 
-- MAGIC To help with all of the complexities, you can use DLT to develop, model, and manage the transformations, pipelines, and Delta Lake tables that will be used by Databricks SQL and Power BI.  
-- MAGIC 
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT7.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Download the Power BI Desktop file to your Windows OS  
-- MAGIC https://github.com/adb-essentials/quickstart-labs/blob/main/ADBQuickStartLabs/DLT%20Demo/QuickStart%20Labs%20Power%20BI%20DLT%20Demo.pbit?raw=true  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open your Power BI Desktop File  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT8.png" width="200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Copy the Server hostName and HTTP Path from the SQL Endpoint into your Power BI parameters. Enter the metastore as hive_metastore and database as dlt_demo. Click OK  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT10.png" width="800">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open a new browser tab. In the lefthand navigation, change the persona switcher to SQL  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DBSQL2.png" width="300">
-- MAGIC 
-- MAGIC ### Click on SQL Endpoints, click on the SQL Endpoint for your lab environment, click on connection details  
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/SQLEndpoint.png" width="800">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### With the Power BI Parameters updated, Click Refresh, you can now browse the dlt_demo database in Power BI    
-- MAGIC <img src="https://publicimg.blob.core.windows.net/images/DLT11.png" width="800">
