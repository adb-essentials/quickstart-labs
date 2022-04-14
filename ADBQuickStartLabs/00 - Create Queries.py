# Databricks notebook source
Databricks_Token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
Workspace = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").getOrElse(None)

# COMMAND ----------

import requests
response = requests.get(
  'https://%s/api/2.0/preview/sql/data_sources' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token}
)

# COMMAND ----------

datasource = response.json()[0]["id"]

# COMMAND ----------

query0 = {
  "data_source_id": datasource,
  "query": """
-- Drop and Create Database
-- DROP DATABASE IF EXISTS kkbox CASCADE;
CREATE DATABASE IF NOT EXISTS kkbox;

-- Copy Into
COPY INTO delta.`/mnt/adbquickstart/bronze/user_log/`
    FROM (SELECT msno::STRING, date:STRING, num_25::INT, num_50::STRING, num_75::INT, num_985::INT, num_100::INT, num_unq::INT, total_secs::DOUBLE
          FROM 'dbfs:/mnt/adbquickstart/user_logs/user_logs_v2.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true',  'dateFormat' = 'yyyyMMdd');

CREATE TABLE kkbox.user_log USING DELTA LOCATION '/mnt/adbquickstart/bronze/user_log/';
    
COPY INTO delta.`/mnt/adbquickstart/bronze/transactions/`
    FROM (SELECT msno::STRING, payment_method_id::INT, payment_plan_days::INT, plan_list_price::INT, actual_amount_paid::INT, is_auto_renew::INT, transaction_date::DATE, membership_expire_date::DATE, is_cancel::INT
          FROM '/mnt/adbquickstart/transactions_v2.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true',  'dateFormat' = 'yyyyMMdd');
    
CREATE TABLE kkbox.transactions USING DELTA LOCATION '/mnt/adbquickstart/bronze/transactions/';

COPY INTO delta.`/mnt/adbquickstart/bronze/members`
    FROM (SELECT msno::STRING, city::INT, bd::INT, gender::STRING, registered_via::INT, registration_init_time::DATE
          FROM 'dbfs:/mnt/adbquickstart/members/members_v3.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true',  'dateFormat' = 'yyyyMMdd');
    
CREATE TABLE kkbox.members USING DELTA LOCATION '/mnt/adbquickstart/bronze/members/';

COPY INTO delta.`/mnt/adbquickstart/bronze/train/`
    FROM (SELECT msno::STRING, is_churn::INT
          FROM 'dbfs:/mnt/adbquickstart/train_v2.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true');
    
CREATE TABLE kkbox.churn USING DELTA LOCATION '/mnt/adbquickstart/bronze/train/';
      

-- Optimze and Order our Tables
OPTIMIZE kkbox.churn ZORDER BY (msno);

OPTIMIZE kkbox.transactions ZORDER BY (msno);

OPTIMIZE kkbox.members ZORDER BY (msno);

OPTIMIZE kkbox.user_log ZORDER BY (msno);

-- Analyze our tables to collect stats
ANALYZE TABLE kkbox.churn COMPUTE STATISTICS;

ANALYZE TABLE kkbox.transactions COMPUTE STATISTICS;

ANALYZE TABLE kkbox.members COMPUTE STATISTICS;

ANALYZE TABLE kkbox.user_log COMPUTE STATISTICS; 
  """,
  "name": "Step 0. Copy Into, Optimize, and Analyze",
  "description": "Create Database, Tables, and run optimizations.",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query0
)

# COMMAND ----------

query1 = {
  "data_source_id": datasource,
  "query": """
SELECT COUNT(1) FROM kkbox.transactions;

-- SELECT * FROM kkbox.transactions;

--SELECT COUNT(1) FROM kkbox.members;

--SELECT * FROM kkbox.members;

-- SELECT COUNT(1) FROM kkbox.user_log

-- SELECT * FROM kkbox.user_log

-- SELECT COUNT(1) FROM kkbox.churn

-- SELECT * FROM kkbox.churn
""",
  "name": "Browse Tables",
  "description": "Browse KKBOX tables.",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query1
)

# COMMAND ----------

response.json()

# COMMAND ----------

query2 = {
  "data_source_id": datasource,
  "query": """SELECT M.city, COUNT(1) As TransactionCount, SUM(T.actual_amount_paid) AS PaidAmount
FROM kkbox.transactions T INNER JOIN kkbox.members M ON T.msno = M.msno
GROUP BY M.city
ORDER BY PaidAmount DESC;""",
  "name": "Paid Amount by City",
  "description": "Paid Amount by City",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query2
)

# COMMAND ----------

response.json()

# COMMAND ----------

query3 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query4 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query5 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query6 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query7 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query8 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query9 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}

# COMMAND ----------

query10 = {
  "data_source_id": datasource,
  "query": """

""",
  "name": "",
  "description": "",
}
