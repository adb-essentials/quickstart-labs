-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Lakehouse Labs for BI Developers using Databricks SQL
-- MAGIC Welcome to the Lakehouse lab for BI developers using Azure Databricks SQL! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Connect Power BI to your DLT database using Databrick SQL
-- MAGIC
-- MAGIC ## The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is create a DLT pipeline which will load a star schema in the Lakehouse to be used with Power BI and other BI tools through Databricks SQL. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Connect to the DLT Database using Databricks SQL and Power BI

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Maintain Star Schemas for Power BI with Delta Live Tables
-- MAGIC To get the optimal performance from Power BI it is recommended to use a star schema data model and to make use of user-defined aggregated tables. However, as you build out your facts, dimensions, and aggregation tables in Delta Lake to be used by the Power BI data model, it can become complex to manage all the pipelines, dependencies, and data quality.  
-- MAGIC
-- MAGIC To help with all of the complexities, you can use DLT to develop, model, and manage the transformations, pipelines, and Delta Lake tables that will be used by Databricks SQL and Power BI.  
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/PBI2.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Download the Power BI Desktop file to your Windows OS  
-- MAGIC https://github.com/adb-essentials/quickstart-labs/raw/main/Lakehouse%20Workshop/05%20-%20BI%20Developer%20Reports/Lakehouse%20Labs%20Power%20BI%20DLT%20Demo.pbit 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open your Power BI Desktop File Template  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/PBI3.png" width="200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Copy the Server hostName and HTTP Path from the SQL Warehouse into your Power BI parameters. Enter the metastore as hive_metastore and database as your variable `UserDB`_DLT. Click Load  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/PBI4.png" width="800">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Open a new browser tab. In the lefthand navigation, change the persona switcher to SQL  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DBSQL2.png" width="300">
-- MAGIC
-- MAGIC ### Click on SQL Warehouses, click on the SQL Warehouse for your lab environment, click on connection details  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/PBI1.png" width="800">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### With the Power BI Parameters updated, Click Refresh, you can now browse the `UserDB` database in Power BI    
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DLT11.png" width="800">
