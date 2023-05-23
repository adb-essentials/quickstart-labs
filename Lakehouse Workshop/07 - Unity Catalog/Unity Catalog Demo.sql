-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Lakehouse Labs for Unity Catalog
-- MAGIC Welcome to the Lakehouse lab for Unity Catalog on Azure Databricks! Over the course of this notebook, you will learn how Unity Catalog enables the folowing features and capabilities for your Lakehouse solutions:
-- MAGIC 1. Creation of managed and external tables
-- MAGIC 2. Sharing those tables accross Databricks workspaces with ACLs  
-- MAGIC 3. Upgrade tables in the HMS and EHMS to Unity Catalog
-- MAGIC 4. See how UC automatically captures lineage information between tables and columns
-- MAGIC 5. Search for data within Unity Catalog
-- MAGIC
-- MAGIC #### The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to **explore Unity Catalog and its features/capabilites**. 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Databricks Unity Catalog - Table ACL
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/us-base-0.png" style="float: right" width="500px"/> 
-- MAGIC
-- MAGIC The main feature of Unity Catalog is to provide you an easy way to setup Table ACL (Access Control Level), but also build Dynamic Views based on each individual permission.
-- MAGIC
-- MAGIC Typically, Analysts will only have access to customers from their country and won't be able to read GDPR/Sensitive informations (like email, firstname etc.)
-- MAGIC
-- MAGIC A typical workflow in the Lakehouse architecture is the following:
-- MAGIC
-- MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
-- MAGIC * Data Scientists can read the final tables and update their features tables
-- MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
-- MAGIC * Data is masked/anonymized dynamically based on each user access level
-- MAGIC
-- MAGIC With Unity Catalog, your tables, users and groups are defined at the account level, cross workspaces. Ideal to deploy and operate a Lakehouse Platform across all your teams.
-- MAGIC
-- MAGIC Let's see how this can be done with the Unity Catalog
-- MAGIC
-- MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Ftable_acl%2Facl&dt=FEATURE_UC_TABLE_ACL">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- DBTITLE 1,Lakehouse Workshop Storage Variables
-- MAGIC %run "/Repos/leo.furlong@databricks.com/quickstart-labs/Lakehouse Workshop/00 - Set Lab Variables"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Notebook Widgets
-- MAGIC The following notebook widgets are being created automatically for you and defaulted to your set variables for easier parameterization of code.  

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.text("Data_PATH_User", Data_PATH_User)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.text("Data_PATH_Ingest", Data_PATH_Ingest)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Workspace Attached to Unity Catalog Metastore
-- MAGIC
-- MAGIC In order to use Unity Catalog, your workspace must be attached to a Unity Catalog Metastore in the Databricks Account Console  
-- MAGIC You can check whether your workspace is attached to a UC Metastore in Data Explorer  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC1.png" width="800px"/> 
-- MAGIC
-- MAGIC If you are a Metastore Admin for the Unity Catalog Metastore that is attached, you click on the widget/link icon to see the metastore details  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC1.2.png" width="800px"/>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Creating the CATALOG
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC
-- MAGIC The first step is to create a new catalog.
-- MAGIC
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC
-- MAGIC To access one table, you can specify the full path: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`
-- MAGIC
-- MAGIC Note that the tables created before Unity Catalog are saved under the catalog named `hive_metastore`. Unity Catalog features are not available for this catalog.
-- MAGIC
-- MAGIC Note that Unity Catalog comes in addition to your existing data, no hard change required!

-- COMMAND ----------

SELECT CURRENT_METASTORE();

-- COMMAND ----------

create catalog if not exists uc_demo

-- COMMAND ----------

use catalog uc_demo;

-- COMMAND ----------

grant usage on catalog uc_demo to `LeoTestGroup`;

-- COMMAND ----------

-- revoke usage on catalog uc_demo from `LeoTestGroup`;

-- COMMAND ----------

show grants on catalog uc_demo;

-- COMMAND ----------

SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating the SCHEMA
-- MAGIC Next, we need to create the SCHEMA (or DATABASE).
-- MAGIC
-- MAGIC Unity catalog provide the standard GRANT SQL syntax. We'll use it to GRANT CREATE and USAGE on our SCHEMA to all the users for this demo.
-- MAGIC
-- MAGIC They'll be able to create extra table into this schema.

-- COMMAND ----------

create database if not exists uc_demo.uc_demo_db;

-- COMMAND ----------

GRANT CREATE, USAGE ON SCHEMA uc_demo.uc_demo_db TO `LeoTestGroup`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating the Catalog and Schema using the UI
-- MAGIC The Data Explorer UI can also be used to create UC Catalogs and Schemas  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC2.png" width="800px"/>
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC3.png" width="400"/> 
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC4.png" width="800px"/>  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC5.png" width="400"/>  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating our tables
-- MAGIC
-- MAGIC We're all set! We can use standard SQL to create our tables.
-- MAGIC
-- MAGIC We'll load some tables that we used during our Data Engineering lab
-- MAGIC
-- MAGIC Note that the table owner is the current user. Owners have full permissions.<br/>
-- MAGIC If you want to change the owner you can set it as following: ```ALTER TABLE <catalog>.<schema>.<table> OWNER TO `<group name>`;```

-- COMMAND ----------

-- DBTITLE 1,PySpark Ingest
-- MAGIC %py 
-- MAGIC Data_PATH_User + '/bronze/transactions'# Read data from parquet files
-- MAGIC transactions_df = (
-- MAGIC   spark.read.parquet(Data_PATH_Ingest + '/transactions')
-- MAGIC     )
-- MAGIC
-- MAGIC # persist in delta lake format
-- MAGIC ( transactions_df
-- MAGIC     .write
-- MAGIC     .format('delta')
-- MAGIC     .mode('overwrite')
-- MAGIC     .saveAsTable("uc_demo.uc_demo_db.transactions")
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE EXTENDED uc_demo.uc_demo_db.transactions

-- COMMAND ----------

-- DBTITLE 1,Auto Loader Ingest
-- MAGIC %py
-- MAGIC # "cloudFiles" indicates the use of Auto Loader
-- MAGIC
-- MAGIC dfBronze = spark.readStream.format("cloudFiles") \
-- MAGIC   .option('cloudFiles.format', 'csv') \
-- MAGIC   .option('header','true') \
-- MAGIC   .schema('msno string, city int, bd int, gender string ,registered_via int , registration_init_time string') \
-- MAGIC   .load(Data_PATH_Ingest + "/members/")
-- MAGIC
-- MAGIC # The stream will shut itself off when it is finished based on the trigger once feature
-- MAGIC # The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
-- MAGIC dfBronze.writeStream \
-- MAGIC   .format("delta") \
-- MAGIC   .trigger(once=True) \
-- MAGIC   .option("checkpointLocation", "abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db_checkpoint/members") \
-- MAGIC   .option('path', "abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/members") \
-- MAGIC   .toTable("uc_demo.uc_demo_db.members")

-- COMMAND ----------

DESCRIBE EXTENDED uc_demo.uc_demo_db.members

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_df.write.format("delta").option("mode","overwrite").save("abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/EY/uc_demo_external_table_filesonly")

-- COMMAND ----------

-- DBTITLE 1,Copy Into Ingest
COPY INTO delta.`abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs`  
    FROM '${Data_PATH_Ingest}/user_logs/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true')

-- COMMAND ----------

SELECT * FROM delta.`abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs`

-- COMMAND ----------

use catalog uc_demo;
grant usage on catalog uc_demo to `LeoTestGroup`;
grant usage on database uc_demo.uc_demo_db to `LeoTestGroup`;
grant usage on database uc_demo.default to `LeoTestGroup`;
grant select on table uc_demo.uc_demo_db.transactions to `LeoTestGroup`;
grant select on table uc_demo.uc_demo_db.members to `LeoTestGroup`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing Permissions in the UI
-- MAGIC UC Metastore Admins and Data Object Owners can see and manage permissions in the Data Explorer UI  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC7.png" width="800px"/>   
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC6.png" width="800px"/>   
-- MAGIC
-- MAGIC Bulk Grants can also be granted to users at higher levels of the hierarchy  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/UC8.png" width="600"/>   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## External Locations

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION `datalake`

-- COMMAND ----------

GRANT READ FILES ON EXTERNAL LOCATION `datalake` TO `LeoTestGroup`;

-- COMMAND ----------

show grants on external location `datalake`

-- COMMAND ----------

list 'abfss://datalake@lafadlspltest.dfs.core.windows.net/'

-- COMMAND ----------

list 'abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db'

-- COMMAND ----------

list 'abfss://kkbox@lafadlspltest.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lineage

-- COMMAND ----------

-- MAGIC %py
-- MAGIC user_logs_df = spark.read.format("delta").load("abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs")
-- MAGIC
-- MAGIC user_logs_summary_df = user_logs_df.groupBy("msno").agg(
-- MAGIC     count("msno").alias("no_transactions"),
-- MAGIC     sum("num_25").alias("Total25"),
-- MAGIC     sum("num_100").alias("Total100"),
-- MAGIC     mean("num_unq").alias("UniqueSongs"),
-- MAGIC     mean("total_secs").alias("TotalSecHeard"),
-- MAGIC )
-- MAGIC
-- MAGIC user_logs_summary_df.write.format('delta') \
-- MAGIC     .mode('overwrite') \
-- MAGIC     .option('path', "abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs_summary") \
-- MAGIC     .saveAsTable("uc_demo.uc_demo_db.user_logs_summary")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS uc_demo.uc_demo_db.member_feature 
USING DELTA
AS 
SELECT 
 mem.msno
,mem.city
,mem.bd
,CASE WHEN mem.gender = 'null' THEN '' ELSE gender END AS gender
,mem.registered_via
,DATE(LEFT(mem.registration_init_time, 4) || '-' || SUBSTR(mem.registration_init_time, 2, 2) || '-' || RIGHT(mem.registration_init_time, 2)) AS registration_init_time
,summ.no_transactions
,summ.Total25
,summ.Total100
,summ.UniqueSongs
,summ.TotalSecHeard
FROM 
uc_demo.uc_demo_db.user_logs_summary summ
INNER JOIN uc_demo.uc_demo_db.members mem ON summ.msno = mem.msno
WHERE mem.bd between 15 and 100

-- COMMAND ----------

WITH member_feature_diff AS (
SELECT 
 mem.msno
,mem.city
,mem.bd
,CASE WHEN mem.gender = 'null' THEN '' ELSE gender END AS gender
,mem.registered_via
,DATE(LEFT(mem.registration_init_time, 4) || '-' || SUBSTR(mem.registration_init_time, 2, 2) || '-' || RIGHT(mem.registration_init_time, 2)) AS registration_init_time
,summ.no_transactions
,summ.Total25
,summ.Total100
,summ.UniqueSongs
,summ.TotalSecHeard
FROM 
uc_demo.uc_demo_db.user_logs_summary summ
INNER JOIN uc_demo.uc_demo_db.members mem ON summ.msno = mem.msno
WHERE mem.bd between 15 and 100
)

MERGE INTO uc_demo.uc_demo_db.member_feature as t
USING member_feature_diff as s
ON t.msno = s.msno
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE VIEW uc_demo.uc_demo_db.member_feature_view 
AS 
SELECT 
 msno
,city
,registered_via
,registration_init_time
,no_transactions
,Total25
,Total100
,UniqueSongs
,TotalSecHeard
FROM 
uc_demo.uc_demo_db.member_feature

-- COMMAND ----------

select * from uc_demo.uc_demo_db.member_feature

-- COMMAND ----------

DESCRIBE EXTENDED uc_demo.uc_demo_db.member_feature 

-- COMMAND ----------

select * from uc_demo.uc_demo_db.member_feature_view

-- COMMAND ----------

use catalog uc_demo;
use database uc_demo_db;
SHOW TABLES

-- COMMAND ----------

use catalog uc_demo;
use database uc_demo_db;
SHOW VIEWS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unity Catalog Lineage Capture
-- MAGIC UC enabled clusters automatically capture lineage at the table level  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Lineage1.png" width="800px"/>   
-- MAGIC
-- MAGIC And even at the column level  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Lineage2.png" width="800px"/> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unity Catalog Data Search
-- MAGIC I can also search Unity Catalog for data with ACLs respected    
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Search.png" width="800px"/> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Upgrade HMS to Unity Catalog
-- MAGIC Finally, I am upgrade my tables from internal or external Hive Metastores to Unity Catalog Metastores  
-- MAGIC
-- MAGIC In Data Explorer, click on a table or database in the hive_metastore that you want to migrate and click `Upgrade`  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Upgrade1.png" width="1000px"/>  
-- MAGIC
-- MAGIC Select the destination catalog and database and click `Next1`  
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Upgrade2.png" width="1000px"/>  
-- MAGIC
-- MAGIC Upgrade the table by clicking the `Run upgrade` button or `Create query for upgrade` button and run the SQL code in Databricks SQL or a Notebook    
-- MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Upgrade3.png" width="1000px"/> 
