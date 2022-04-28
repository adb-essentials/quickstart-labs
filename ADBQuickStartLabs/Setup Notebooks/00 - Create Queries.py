# Databricks notebook source
# MAGIC %md
# MAGIC ### STOP, do not run this notebook individually. This notebook will be executed from parent notebooks at the proper time. 

# COMMAND ----------

dbutils.widgets.text("Databricks_Token", "", "Databricks_Token")

# COMMAND ----------

Databricks_Token = dbutils.widgets.get("Databricks_Token")

# COMMAND ----------

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
    FROM (SELECT msno::STRING, date::STRING, num_25::INT, num_50::STRING, num_75::INT, num_985::INT, num_100::INT, num_unq::INT, total_secs::DOUBLE
          FROM 'dbfs:/mnt/adbquickstart/user_logs/user_logs_v2.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true',  'dateFormat' = 'yyyyMMdd');

CREATE TABLE kkbox.user_log USING DELTA LOCATION '/mnt/adbquickstart/bronze/user_log/';
    
COPY INTO delta.`/mnt/adbquickstart/bronze/transactions/`
    FROM (SELECT msno::STRING, payment_method_id::INT, payment_plan_days::INT, plan_list_price::INT, actual_amount_paid::INT, is_auto_renew::INT, transaction_date::STRING, membership_expire_date::STRING, is_cancel::INT
          FROM '/mnt/adbquickstart/transactions_v2.csv')
    FILEFORMAT = CSV
    FORMAT_OPTIONS('header' = 'true',  'dateFormat' = 'yyyyMMdd');
    
CREATE TABLE kkbox.transactions USING DELTA LOCATION '/mnt/adbquickstart/bronze/transactions/';

COPY INTO delta.`/mnt/adbquickstart/bronze/members`
    FROM (SELECT msno::STRING, city::INT, bd::INT, gender::STRING, registered_via::INT, registration_init_time::STRING
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

-- Cache our tables to the result set cache
SELECT * FROM kkbox.churn;

SELECT * FROM kkbox.transactions;

SELECT * FROM kkbox.members;

SELECT * FROM kkbox.user_log; 
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
-- Let's quickly explore our data
-- We can also explore our data using the Data Explorer UI

SELECT COUNT(1) FROM kkbox.transactions;

-- SELECT * FROM kkbox.transactions;

-- SELECT COUNT(1) FROM kkbox.members;

-- SELECT * FROM kkbox.members;

-- SELECT COUNT(1) FROM kkbox.user_log;

-- SELECT * FROM kkbox.user_log;

-- SELECT COUNT(1) FROM kkbox.churn;

-- SELECT * FROM kkbox.churn;
""",
  "name": "Step1. Browse Tables",
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

query2 = {
  "data_source_id": datasource,
  "query": """
-- Which cities have the most transactions and amount paid?

SELECT M.city, COUNT(1) As TransactionCount, SUM(T.actual_amount_paid) AS PaidAmount
FROM kkbox.transactions T INNER JOIN kkbox.members M ON T.msno = M.msno
GROUP BY M.city
ORDER BY PaidAmount DESC;""",
  "name": "Step2. Paid Amount by City",
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

query3 = {
  "data_source_id": datasource,
  "query": """
-- Are our subscribers growing or shrinking?

SELECT date_format(to_date(registration_init_time,'yyyyMMdd'),'y-MM') AS registration_month, 
  count(*) AS members
FROM kkbox.members
GROUP BY registration_month 
ORDER BY registration_month ASC
""",
  "name": "Step3. Members By Registration Month",
  "description": "Members By Registration Month",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query3
)

# COMMAND ----------

query4 = {
  "data_source_id": datasource,
  "query": """
-- The data below indicate that we were having steady month-over-month growth until early 2016, but our growth rate has been steadily declining since then.

WITH new_monthly_users AS (
  SELECT
    date_format(to_date(registration_init_time, 'yyyyMMdd'), 'y-MM') AS registration_month,
    count(*) AS new_members
  FROM
    kkbox.members
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
""",
  "name": "Step4. Monthly User Growth Rate",
  "description": "Monthly User Growth Rate",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query4
)

# COMMAND ----------

query5 = {
  "data_source_id": datasource,
  "query": """
-- What is the breakdown of new (recently subscribed) vs. old (subscribed 1 or more years ago) customers?
-- Most of our current subscribers became customers in 2016, with 2013-2015 close behind.

SELECT year(to_date(registration_init_time,'yyyyMMdd')) AS registration_year,
  count(*) AS members
FROM kkbox.members
GROUP BY registration_year
HAVING registration_year >= 2010
""",
  "name": "Step5. Members by Registration Year",
  "description": "Members by Registration Year",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query5
)

# COMMAND ----------

query6 = {
  "data_source_id": datasource,
  "query": """
-- What number of user transactions are new subscriptions vs. renewals vs. cancellations?
-- The data below indicates that we have a healthy renewal and new subscriber rate, however our churn rate seems to be increasing.

WITH member_transactions AS (
  SELECT date_format(to_date(m.registration_init_time,'yyyyMMdd'),'y-MM') AS registration_month,
    date_format(to_date(transaction_date,'yyyyMMdd'),'y-MM') AS transaction_month,
    is_auto_renew, is_cancel
  FROM kkbox.transactions t JOIN kkbox.members m ON (t.msno = m.msno)
  )
SELECT 
  transaction_month, 
  sum(is_cancel) cancellations, 
  sum(is_auto_renew) renewals, 
  sum(CASE WHEN is_cancel = 0 AND is_auto_renew = 0 THEN 1 ELSE 0 END) AS new_subscriptions
FROM member_transactions
GROUP BY transaction_month
ORDER BY transaction_month ASC
""",
  "name": "Step6. Breakdown of Monthly Renewals, Cancellations and New Subscriptions",
  "description": "Breakdown of Monthly Renewals, Cancellations and New Subscriptions",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query6
)

# COMMAND ----------

query7 = {
  "data_source_id": datasource,
  "query": """
-- What is the distribution of payment amount among our subscribers
-- Does subscriber location impact the payment amount?  Create a box plot to visualize the data
-- As shown below, the location of our subscribers seems to have an effect on the amount paid and subscription length. 
-- Perhaps we can focus our marketing efforts in higher revenue locations.

SELECT city, int(actual_amount_paid) 
FROM kkbox.transactions t JOIN kkbox.members m ON (t.msno = m.msno)
""",
  "name": "Step7. Distribution of Payment",
  "description": "Distribution of Payment",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query7
)

# COMMAND ----------

query8 = {
  "data_source_id": datasource,
  "query": """
-- What is the distribution of daily listening time among our subscribers? Does subscriber location impact the listening time?
-- Histograms are useful in understanding the distribution of a particular attribute in the data. We we below that 1) the listening time has a long tail distribution and 2) the location of the subscriber does not drasitically change the distribution.

SELECT city, 
  int(total_secs) AS listening_time 
FROM kkbox.user_log l 
  JOIN kkbox.members m ON (l.msno = m.msno) 
WHERE total_secs < 50000
""",
  "name": "Step8. Distribution of Daily Listening",
  "description": "Distribution of Daily Listening",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query8
)

# COMMAND ----------

query9 = {
  "data_source_id": datasource,
  "query": """
-- Do subscribers who have been customers for a longer time use the platform more? 
-- Does it impact their likelihood to renew their subscription?
-- Below we see that there is more activity from younger customers (ie. those that have been subscribers for 5 years or less), 
-- but there does not appear to be any relationship with the listening time or subscriber days to whether a subscriber will cancel their subscription.

WITH churned_subscribers AS (
SELECT DISTINCT msno, is_cancel churned FROM kkbox.transactions
)
SELECT l.msno, churned,
  datediff(to_date(l.date,'yyyyMMdd'),to_date(m.registration_init_time,'yyyyMMdd')) AS days_subscriber,
  avg(l.total_secs) AS daily_listen_time
FROM kkbox.user_log l JOIN kkbox.members m ON (l.msno = m.msno)
  JOIN churned_subscribers c ON (l.msno = c.msno)
GROUP BY l.msno, days_subscriber, churned
""",
  "name": "Step9. Likelihood to Renew their Subscription",
  "description": "Likelihood to Renew their Subscription",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query9
)
