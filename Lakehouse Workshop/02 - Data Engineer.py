# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Lakehouse Lab for Data Engineers
# MAGIC Welcome to the Lakehouse lab for data engineers on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
# MAGIC 1. Access your enterprise data lake in Azure using Databricks
# MAGIC 2. Transform and store your data in a reliable and performant Delta Lake
# MAGIC 3. Use Update,Delete,Merge,Schema Evolution and Time Travel Capabilities, CDF (Change Data Feed) of Delta Lake
# MAGIC
# MAGIC ## The Use Case
# MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

# COMMAND ----------

# DBTITLE 1,Lakehouse Workshop Storage Variables
# MAGIC %run "../Lakehouse Workshop/00 - Set Lab Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Widgets
# MAGIC The following notebook widgets are being created automatically for you and defaulted to your set variables for easier parameterization of code.  

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("UserDB", UserDB)

# COMMAND ----------

dbutils.widgets.text("Data_PATH_User", Data_PATH_User)

# COMMAND ----------

dbutils.widgets.text("Data_PATH_Ingest", Data_PATH_Ingest)

# COMMAND ----------

# DBTITLE 1,Delete existing files
# delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS {0} CASCADE'.format(UserDB))

# drop any old delta lake files that might have been created
dbutils.fs.rm(Data_PATH_User, recurse=True)
# create database to house SQL tables
_ = spark.sql('CREATE DATABASE {0} LOCATION "{1}"'.format(UserDB, Data_PATH_User))

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- #DATA ENGINEERING AND STREAMING ARCHITECTURE -->
# MAGIC <!-- <img src="https://kpistoropen.blob.core.windows.net/collateral/quickstart/etl.png" width=1500> -->
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/c8be0896dc688c045ec3866e1fc744981f47b844/images/DEandStreaming.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC We can view and navigate the contents of our container using Databricks `%fs` [file system commands](https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utility-dbutilsfs).

# COMMAND ----------

dbutils.fs.ls(Data_PATH_Ingest)

# COMMAND ----------

dbutils.fs.ls(Data_PATH_User)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Your Data
# MAGIC In 2018, [KKBox](https://www.kkbox.com/) - a popular music streaming service based in Taiwan - released a [dataset](https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data) consisting of a little over two years of (anonymized) customer transaction and activity data with the goal of challenging the Data & AI community to predict which customers would churn in a future period.  
# MAGIC
# MAGIC The primary data files are organized in the storage container:
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/kkbox_filedownloads.png' width=150>
# MAGIC
# MAGIC Read into dataframes, these files form the following data model:
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/kkbox_schema.png' width=150>
# MAGIC
# MAGIC Each subscriber is uniquely identified by a value in the `msno` field of the `members` table. Data in the `transactions` and `user_logs` tables provide a record of subscription management and streaming activities, respectively.  

# COMMAND ----------

# MAGIC %md
# MAGIC ##In this Demo notebook we will showcase some of the most common scenarios Data Engineers encouter while working on ingesting and data processing
# MAGIC ####1. Ingest Data in Batch Process
# MAGIC ####2. Ingest Data using Autoloader and COPY INTO command
# MAGIC ####3. Perform operations such as Update, Merge , Delete on delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCENARIO 1  : INGEST DATA in BATCH PROCESS (Parquet Files)
# MAGIC ##### In this scenario we will ingest an inital load of transactional data to Delta format. We will ingest one data sets : (Transaction Dataset : Parquet Format) and convert it to Delta(bronze layer)

# COMMAND ----------

# DBTITLE 1,Prep Transactions Dataset - Parquet Files to Delta
Data_PATH_User + '/bronze/transactions'# Read data from parquet files
transactions = (
  spark.read.parquet(Data_PATH_Ingest + '/transactions')
    )

# persist in delta lake format
( transactions
    .write
    .format('delta')
    .partitionBy('transaction_date')
    .mode('overwrite')
    .save(Data_PATH_User + '/bronze/transactions')
  )

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE {0}.transactions
  USING DELTA 
  LOCATION "{1}/bronze/transactions"
  '''.format(UserDB, Data_PATH_User))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.transactions

# COMMAND ----------

# DBTITLE 1,Let's look at the Delta Files
dbutils.fs.ls(Data_PATH_User + '/bronze/transactions')

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how the partitionBy setting wrote out the data to different directories by the transaction date. The actual Parquet files are in the sub-directories. This technique is known as data partitioning and it is generally used to optimize working with large datasets.  
# MAGIC **However, Databricks doesn't recommend that you partition your data until the dataset is at least 1TB in size as Delta Lake Optimize and ZOrder operations provide better performance than partitioning;** this was just an example.

# COMMAND ----------

# DBTITLE 1,Files in the partitioned directory
dbutils.fs.ls(Data_PATH_User + '/bronze/transactions/transaction_date=2015-01-09/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCENARIO 2  : INGEST DATA with Databricks AutoLoader and COPY INTO

# COMMAND ----------

# MAGIC %md
# MAGIC ####Auto Loader, COPY INTO and Incrementally Ingesting Data
# MAGIC Auto Loader and COPY INTO are two methods of ingesting data into a Delta Lake table from a folder in a Data Lake. “Yeah, so... Why is that so special?”, you may ask. The reason these features are special is that they make it possible to ingest data directly from a data lake incrementally, in an idempotent way, without needing a distributed streaming system like Kafka. This can considerably simplify the Incremental ETL process. It is also an extremely efficient way to ingest data since you are only ingesting new data and not reprocessing data that already exists. Below is an Incremental ETL architecture. We will focus on the left hand side, ingesting into tables from outside sources. 
# MAGIC
# MAGIC You can incrementally ingest data either continuously or scheduled in a job. COPY INTO and Auto Loader cover both cases and we will show you how below.
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/get-start-delta-blog-img-1.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview Autoloader
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html) is an ingest feature of Databricks that makes it simple to incrementally ingest only new data from Azure Data Lake. In this notebook we will use Auto Loader for a basic ingest use case but there are many features of Auto Loader, like [schema inference and evolution](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html#schema-inference-and-evolution), that make it possible to ingest very complex and dynymically changing data

# COMMAND ----------

# DBTITLE 1,Prep Members Dataset - Ingest via Autoloader
# "cloudFiles" indicates the use of Auto Loader

dfBronze = spark.readStream.format("cloudFiles") \
  .option('cloudFiles.format', 'csv') \
  .option('header','true') \
  .schema('msno string, city int, bd int, gender string ,registered_via int , registration_init_time string') \
  .load(Data_PATH_Ingest + "/members/")

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
dfBronze.writeStream \
  .format("delta") \
  .trigger(once=True) \
  .option("checkpointLocation", Data_PATH_User + "/checkpoint/members") \
  .toTable(UserDB + ".members")
# .start(Data_PATH_User + "/bronze/members")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.members

# COMMAND ----------

# DBTITLE 1,Cool.. Lets see if we could see the files in the member delta folder
dbutils.fs.ls(Data_PATH_User + "/members")

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that even though we didn't specify our location, our data was saved as an external table using the default location of the database. If we had not set the default location of the database, the data would have been stored as a managed table.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### INGEST DATA with Databricks COPY INTO

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO delta.`${Data_PATH_User}/bronze/user_log/`  
# MAGIC     FROM '${Data_PATH_Ingest}/user_logs/'
# MAGIC     FILEFORMAT = CSV
# MAGIC     FORMAT_OPTIONS('header' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${UserDB}.user_log
# MAGIC USING DELTA 
# MAGIC LOCATION '${Data_PATH_User}/bronze/user_log/'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.user_log

# COMMAND ----------

# DBTITLE 1,Great !!! Now lets read the data and see if everything went well.
## Read the Bronze Data
transactions_bronze = spark.read.format("delta").load(Data_PATH_User + '/bronze/transactions')
members_bronze = spark.read.format("delta").load(Data_PATH_User + "/members")
user_logs_bronze = spark.read.format("delta").load(Data_PATH_User + '/bronze/user_log/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3 - Delta Lake Features
# MAGIC In the following sections we'll focus on Delta Lake features using the Members table. We'll create a Gold table (aggregated or data model table) for our testing. In a real-world scenario, we'd likely have a Silver table in between Bronze and Gold that might have standardization, data clean-up, additional shape/structure, and even business rules applied.

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS ${UserDB}.members_gold

# COMMAND ----------

# dbutils.fs.rm(Data_PATH_User + "/gold/members/",recurse=True)

# COMMAND ----------

# DBTITLE 1,Members by Registration Year - Create a Gold table
members_transform = members_bronze.withColumn('years',members_bronze['registration_init_time'].substr(1, 4))

members_gold = members_transform.groupBy('years').count()

members_gold.createOrReplaceTempView("member_gold")

#Save our Gold table in Delta format and Enable CDC on the Delta Table

# members_gold.write.format('delta').mode('overwrite').save(Data_PATH_User + '/gold/members/')
members_gold.write.format('delta').mode('overwrite').option('path', Data_PATH_User + '/gold/members/').saveAsTable(UserDB + '.members_gold')

display(members_gold)

# COMMAND ----------

# DBTITLE 1,Query Gold table using file path
# MAGIC %sql
# MAGIC SELECT * from delta.`${Data_PATH_User}/gold/members/` 

# COMMAND ----------

# DBTITLE 1,Query Gold table
# MAGIC %sql
# MAGIC select * from ${UserDB}.members_gold

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Delta as Unified Batch and Streaming Source and Sink
# MAGIC
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `members_gold` table
# MAGIC * We will run two streaming queries concurrently against this data and update the table

# COMMAND ----------

# DBTITLE 1,Stop the notebook before the streaming cell, in case of a "run all" 
dbutils.notebook.exit("stop") 

# COMMAND ----------

# Read the insertion of data
members_gold_readStream = spark.readStream.format("delta").load(Data_PATH_User + '/gold/members/')
members_gold_readStream.createOrReplaceTempView("members_gold_readStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT years, sum(`count`) AS members
# MAGIC FROM members_gold_readStream
# MAGIC GROUP BY years
# MAGIC ORDER BY years

# COMMAND ----------

# DBTITLE 1,Insert new rows - Second Stream
import time
i = 1
while i <= 6:
  # Execute Insert statement
  insert_sql = "INSERT INTO {0}.members_gold VALUES (2004, 450000)".format(UserDB)
  spark.sql(insert_sql)
  print('members_gold_delta: inserted new row of data, loop: [%s]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform DML operations , Schema Evolution and Time Travel
# MAGIC #####Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing data engineers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md ### DELETE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table to remove records from year 2009
# MAGIC DELETE FROM ${UserDB}.members_gold WHERE years = 2009

# COMMAND ----------

# DBTITLE 1,Let's confirm the data is deleted for year 2009
# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.members_gold
# MAGIC ORDER BY years

# COMMAND ----------

# MAGIC %md ### UPDATE Support

# COMMAND ----------

# DBTITLE 1,Let's update the count for year 2010
# MAGIC %sql
# MAGIC UPDATE ${UserDB}.members_gold SET `count` = 50000 WHERE years = 2010

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.members_gold
# MAGIC ORDER BY years

# COMMAND ----------

# MAGIC %md ###MERGE INTO Support
# MAGIC
# MAGIC #### INSERT or UPDATE with Delta Lake: 2-step process
# MAGIC
# MAGIC With Delta Lake, inserting or updating a table is a simple 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use the `MERGE` command

# COMMAND ----------

# DBTITLE 1,Let's create a simple table to merge
items = [(2009, 50000), (2021, 250000), (2012, 35000)]
cols = ['years', 'count']
merge_table = spark.createDataFrame(items, cols)
merge_table.createOrReplaceTempView("merge_table")
display(merge_table)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ${UserDB}.members_gold as d
# MAGIC USING merge_table as m
# MAGIC on d.years = m.years
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Perfect!! Let's check to make sure it worked
# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.members_gold
# MAGIC ORDER BY years

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# DBTITLE 1,Generate a new "usage" column in a dummy table
member_dummy = spark.sql("SELECT years, count, CAST(rand(10) * 10 * count AS double) AS usage FROM {0}.members_gold".format(UserDB))
display(member_dummy)

# COMMAND ----------

# DBTITLE 1,Merge the schema to the delta table
# Add the mergeSchema option
member_dummy.write.option("mergeSchema","true").format("delta").mode("append").save(Data_PATH_User + '/gold/members/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${UserDB}.members_gold

# COMMAND ----------

# MAGIC %md ###Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# DBTITLE 1,Review Delta Lake Table History
# MAGIC %sql
# MAGIC DESCRIBE HISTORY ${UserDB}.members_gold

# COMMAND ----------

# MAGIC %md ####  Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# DBTITLE 1,Let's look at the version 0 - When the table was created
# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.members_gold VERSION AS OF 0
# MAGIC order by years

# COMMAND ----------

# MAGIC %md
# MAGIC ### I can even roll back my table

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE ${UserDB}.members_gold TO VERSION AS OF 9

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.members_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### Or even create shallow or deep clones of a table (backups or testing tables)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta.`${Data_PATH_User}/gold/members_back/`
# MAGIC DEEP CLONE delta.`${Data_PATH_User}/gold/members/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${Data_PATH_User}/gold/members_back/`

# COMMAND ----------

# DBTITLE 0,Simplify Your Medallion Architecture with Delta Lake’s CDF Featurentitled
# MAGIC %md 
# MAGIC ### Simplify Your Medallion Architecture with Delta Lake’s CDF Feature
# MAGIC
# MAGIC ### Overview
# MAGIC The medallion architecture takes raw data landed from source systems and refines the data through bronze, silver and gold tables. It is an architecture that the MERGE operation and log versioning in Delta Lake make possible. Change data capture (CDC) is a use case that we see many customers implement in Databricks. We are happy to announce an exciting new Change data feed (CDF) feature in Delta Lake that makes this architecture even simpler to implement!
# MAGIC
# MAGIC The following example ingests financial data. Estimated Earnings Per Share (EPS) is financial data from analysts predicting what a company’s quarterly earnings per share will be. The raw data can come from many different sources and from multiple analysts for multiple stocks. The data is simply inserted into the bronze table, it will  change in the silver and then aggregate values need to be recomputed in the gold table based on the changed data in the silver. 
# MAGIC
# MAGIC While these transformations can get complex, thankfully now the row based CDF feature can be simple and efficient but how do you use it? Let’s dig in!
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/05/cdf-blog-img-1-rev.png" width=600>

# COMMAND ----------

dbutils.fs.ls(Data_PATH_User + '/gold/members/')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('${UserDB}.members_gold', 1) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### IDENTITY COLUMN
# MAGIC Delta Lake now supports identity columns. When you write to a Delta table that defines an identity column, and you do not provide values for that column, Delta now automatically assigns a unique and statistically increasing or decreasing value.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${UserDB}.members_new
# MAGIC ( ID BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   years STRING, 
# MAGIC   count LONG
# MAGIC )
# MAGIC USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${UserDB}.members_new (years, count) TABLE member_gold

# COMMAND ----------

# DBTITLE 1,Let's look at the identity column
# MAGIC %sql 
# MAGIC select * from ${UserDB}.members_new;

# COMMAND ----------

# DBTITLE 1,Finally !!! Lets End this with with some performance enhancement feature 
# MAGIC %md #####  OPTIMIZE (Delta Lake on Databricks)
# MAGIC Optimizes the layout of Delta Lake data. Optionally, optimize a subset of data or colocate data by column. If you do not specify colocation, bin-packing optimization is performed.

# COMMAND ----------

# MAGIC %sql OPTIMIZE ${UserDB}.user_log ZORDER BY (date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANALYZE TABLE
# MAGIC The ANALYZE TABLE statement collects statistics about one specific table or all the tables in one specified database, that are to be used by the query optimizer to find a better query execution plan

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE ${UserDB}.user_log COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLES IN ${UserDB} COMPUTE STATISTICS;

# COMMAND ----------


