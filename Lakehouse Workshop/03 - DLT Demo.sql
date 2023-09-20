-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Lakehouse Lab for Data Engineers on Delta Live Tables
-- MAGIC Welcome to the Lakehouse lab for data engineers on Azure Databricks for Delta Live Tables (DLT)! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Create a DLT notebook with Data Quality rules
-- MAGIC 2. Create a DLT Pipelines and run it
-- MAGIC
-- MAGIC ## The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is create a DLT pipeline which will load a star schema in the Lakehouse to be used with Power BI and other BI tools through Databricks SQL. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Delta Live Tables Simplifies your Batch and Streaming ETL Pipelines  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DLT0.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a DLT notebook with Data Quality rules

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse the Delta Live Tables code in the following notebook  
-- MAGIC ..Lakehouse Workshop/03 - DLT Demo/DLT Code
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLTCode1.2.png" width="300">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a Delta Live Tables Pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Navigate to Delta Live Tables
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLT1.png" width="150">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new Delta Live Tables Pipeline  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLT2.1.png" width="1000">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Configure your Delta Live Tables Pipeline using the following configurations  
-- MAGIC
-- MAGIC **Pipeline name:** LakehouseWorkshopDLT_`User`  
-- MAGIC **Product edition:** Advanced  
-- MAGIC **Pipeline mode:** Triggered    
-- MAGIC **Source code:** browse to the DLT Notebook at ../Lakehouse-Workshop/Lakehouse Workshop/03 - DLT Demo Code/DLT Code   
-- MAGIC **Storage options:** Unity Catalog  
-- MAGIC **Catalog:** lakehouselabs    
-- MAGIC **Target schema:** `UserDB`_DLT    
-- MAGIC **Cluster mode:** Enanced autoscaling  
-- MAGIC **Min workers:** 1  
-- MAGIC **Max workers:** 2  
-- MAGIC **Use Photon Acceleration:**  check   
-- MAGIC **Click Create**  
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLT3.1.png" width="800">  
-- MAGIC
-- MAGIC   
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLT4.1.png" width="500">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Start your new DLT pipeline  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLT5.1.png" width="1200">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Browse the lineage and data quality information of your completed DLT pipeline  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/private-env/images/DLT6.1.png" width="1200">
