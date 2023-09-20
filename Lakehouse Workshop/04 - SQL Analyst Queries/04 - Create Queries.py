# Databricks notebook source
# MAGIC %md
# MAGIC ### STOP, do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

# MAGIC %run "../00 - Set Lab Variables"

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC Lookup the Deployment Folder ID by getting the Notebook/Folder URL from the UI

# COMMAND ----------

folderPath = { "path": "/databrickslabs/" + User }

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/workspace/mkdirs' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=folderPath
)

# COMMAND ----------

response = requests.get(
  'https://%s/api/2.0/workspace/get-status' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=folderPath
)

# COMMAND ----------

folder_deployment_id = str(response.json()["object_id"])

# COMMAND ----------

response = requests.get(
  'https://%s/api/2.0/preview/sql/data_sources' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token}
)

# COMMAND ----------

datasource = response.json()[0]["id"]

# COMMAND ----------

print(datasource)

# COMMAND ----------

query0 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- We assume that you've already run notebook "02 - Data Engineer" to create most of the base tables

DROP TABLE IF EXISTS bronze_train;

CREATE TABLE bronze_train;

COPY INTO bronze_train
    FROM (SELECT msno::STRING, is_churn::INT
          FROM '{1}/train_v2.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true');
          
-- Optimze and Order our Tables
OPTIMIZE silver_churndata ZORDER BY (msno);

OPTIMIZE bronze_transactions ZORDER BY (msno);

OPTIMIZE bronze_members ZORDER BY (msno);

OPTIMIZE bronze_user_log ZORDER BY (msno);

-- Analyze our tables to collect stats
ANALYZE TABLE silver_churndata COMPUTE STATISTICS;

ANALYZE TABLE bronze_transactions COMPUTE STATISTICS;

ANALYZE TABLE bronze_members COMPUTE STATISTICS;

ANALYZE TABLE bronze_user_log COMPUTE STATISTICS; 

-- Cache our tables to the SSD cache
SELECT * FROM silver_churndata;

SELECT * FROM bronze_transactions;

SELECT * FROM bronze_members;

SELECT * FROM bronze_user_log; 
  """.format(UserDB, Data_PATH_Ingest),
  "name": "Step0. Copy Into, Optimize, and Analyze",
  "description": "Create Database, Tables, and run optimizations.",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query0
)

# COMMAND ----------

query1 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- Let's quickly explore our data
-- We can also explore our data using the Data Explorer UI

SELECT COUNT(1) FROM bronze_transactions;

-- SELECT * FROM transactions;

-- SELECT COUNT(1) FROM members;

-- SELECT * FROM members;

-- SELECT COUNT(1) FROM user_log;

-- SELECT * FROM user_log;

-- SELECT COUNT(1) FROM churn;

-- SELECT * FROM churn;

""".format(UserDB),
  "name": "Step1. Browse Tables",
  "description": "Browse {0} tables.",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query1
)

# COMMAND ----------

query2 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- Which cities have the most transactions and amount paid?

SELECT M.city, COUNT(1) As TransactionCount, SUM(T.actual_amount_paid) AS PaidAmount
FROM bronze_transactions T INNER JOIN bronze_members M ON T.msno = M.msno
GROUP BY M.city
ORDER BY PaidAmount DESC;
""".format(UserDB),
  "name": "Step2. Paid Amount by City",
  "description": "Paid Amount by City",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query2
)

# COMMAND ----------

query3 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- Are our subscribers growing or shrinking?

SELECT date_format(to_date(registration_init_time,'yyyyMMdd'),'y-MM') AS registration_month, 
  count(*) AS members
FROM bronze_members
GROUP BY registration_month 
ORDER BY registration_month ASC
""".format(UserDB),
  "name": "Step3. Members By Registration Month",
  "description": "Members By Registration Month",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query3
)

# COMMAND ----------

query4 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- The data below indicate that we were having steady month-over-month growth until early 2016, but our growth rate has been steadily declining since then.

WITH new_monthly_users AS (
  SELECT
    date_format(to_date(registration_init_time, 'yyyyMMdd'), 'y-MM') AS registration_month,
    count(*) AS new_members
  FROM
    bronze_members
  GROUP BY
    registration_month
)
SELECT
  registration_month,
  new_members,
  (sum(new_members) OVER (ORDER BY registration_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS running_total,
  new_members / (sum(new_members) OVER (ORDER BY registration_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS growth
FROM
  new_monthly_users
ORDER BY
  registration_month

""".format(UserDB),
  "name": "Step4. Monthly User Growth Rate",
  "description": "Monthly User Growth Rate",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query4
)

# COMMAND ----------

query5 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- What is the breakdown of new (recently subscribed) vs. old (subscribed 1 or more years ago) customers?
-- Most of our current subscribers became customers in 2016, with 2013-2015 close behind.

SELECT year(to_date(registration_init_time,'yyyyMMdd')) AS registration_year,
  count(*) AS members
FROM bronze_members
GROUP BY registration_year
HAVING registration_year >= 2010
""".format(UserDB),
  "name": "Step5. Members by Registration Year",
  "description": "Members by Registration Year",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query5
)

# COMMAND ----------

query6 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- What number of user transactions are new subscriptions vs. renewals vs. cancellations?
-- The data below indicates that we have a healthy renewal and new subscriber rate, however our churn rate seems to be increasing.

WITH member_transactions AS (
  SELECT date_format(to_date(m.registration_init_time,'yyyyMMdd'),'y-MM') AS registration_month,
    date_format(to_date(transaction_date,'yyyyMMdd'),'y-MM') AS transaction_month,
    is_auto_renew, is_cancel
  FROM bronze_transactions t JOIN bronze_members m ON (t.msno = m.msno)
  )
SELECT 
  transaction_month, 
  sum(is_cancel) cancellations, 
  sum(is_auto_renew) renewals, 
  sum(CASE WHEN is_cancel = 0 AND is_auto_renew = 0 THEN 1 ELSE 0 END) AS new_subscriptions
FROM member_transactions
GROUP BY transaction_month
ORDER BY transaction_month ASC

""".format(UserDB),
  "name": "Step6. Breakdown of Monthly Renewals, Cancellations and New Subscriptions",
  "description": "Breakdown of Monthly Renewals, Cancellations and New Subscriptions",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query6
)

# COMMAND ----------

query7 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- What is the distribution of payment amount among our subscribers
-- Does subscriber location impact the payment amount?  Create a box plot to visualize the data
-- As shown below, the location of our subscribers seems to have an effect on the amount paid and subscription length. 
-- Perhaps we can focus our marketing efforts in higher revenue locations.

SELECT city, int(actual_amount_paid) 
FROM bronze_transactions t JOIN bronze_members m ON (t.msno = m.msno)
""".format(UserDB),
  "name": "Step7. Distribution of Payment",
  "description": "Distribution of Payment",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query7
)

# COMMAND ----------

query8 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- What is the distribution of daily listening time among our subscribers? Does subscriber location impact the listening time?
-- Histograms are useful in understanding the distribution of a particular attribute in the data. We we below that 1) the listening time has a long tail distribution and 2) the location of the subscriber does not drasitically change the distribution.

SELECT city, 
  int(total_secs) AS listening_time 
FROM bronze_user_log l 
  JOIN bronze_members m ON (l.msno = m.msno) 
WHERE total_secs < 50000
""".format(UserDB),
  "name": "Step8. Distribution of Daily Listening",
  "description": "Distribution of Daily Listening",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query8
)

# COMMAND ----------

query9 = {
  "data_source_id": datasource,
  "parent": "folders/" + folder_deployment_id,
  "query": """
USE CATALOG lakehouselabs;
USE {0};

-- Do subscribers who have been customers for a longer time use the platform more? 
-- Does it impact their likelihood to renew their subscription?
-- Below we see that there is more activity from younger customers (ie. those that have been subscribers for 5 years or less), 
-- but there does not appear to be any relationship with the listening time or subscriber days to whether a subscriber will cancel their subscription.

WITH churned_subscribers AS (
SELECT DISTINCT msno, is_cancel churned FROM bronze_transactions
)
SELECT l.msno, churned,
  datediff(to_date(l.date,'yyyyMMdd'),to_date(m.registration_init_time,'yyyyMMdd')) AS days_subscriber,
  avg(l.total_secs) AS daily_listen_time
FROM bronze_user_log l JOIN bronze_members m ON (l.msno = m.msno)
  JOIN churned_subscribers c ON (l.msno = c.msno)
GROUP BY l.msno, days_subscriber, churned

""".format(UserDB),
  "name": "Step9. Likelihood to Renew their Subscription",
  "description": "Likelihood to Renew their Subscription",
}

# COMMAND ----------

response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query9
)
