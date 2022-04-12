-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure Databricks Quickstart for Data Analysts
-- MAGIC Welcome to the quickstart lab for data analysts on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
-- MAGIC 1. Access your enterprise data lake in Azure using Databricks
-- MAGIC 2. Explore data sets using SQL powered by a highly optimized Databricks Spark
-- MAGIC 3. Transform and store your data in a reliable and performance Delta Lake
-- MAGIC 
-- MAGIC ## The Use Case
-- MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Accessing Your Enterprise Data Lake
-- MAGIC Databricks enables an architecture where your analytics is decoupled from your data storage. This allows organizations to store their data cost effectively in Azure Storage and share their data across best of breed tools in Azure without duplicating it in data silos. 
-- MAGIC 
-- MAGIC <img src="https://sguptasa.blob.core.windows.net/random/Delta%20Lakehouse.png" width=800>
-- MAGIC 
-- MAGIC In this notebook, we focus exclusively on the **SQL Analytics** user. Subsequent quickstart labs will demonstrate data science, data engineering and machine learning on the same data set without duplicating it. 
-- MAGIC 
-- MAGIC Run the code below to set up your storage access in Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **IMPORTANT: Before you start, copy and paste your lab environment's storage information into the widgets at the top of the `00 - Setup Storage` notebook.**

-- COMMAND ----------

-- MAGIC %run "./00 - Setup Storage"

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
-- MAGIC We can query our data lake using SQL to reference the data locations directly

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adbquickstart/KKBox-Dataset-orig/members`

-- COMMAND ----------

-- MAGIC %md Alternatively, we can create a logical view on the data to make it easier to query

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW members_raw
USING CSV
OPTIONS (
  -- Location of our CSV data
  path '/mnt/adbquickstart/KKBox-Dataset-orig/members', 
  -- Header in the file
  header True);
  
SELECT * FROM members_raw

-- COMMAND ----------

-- MAGIC %md Let's create views for `transactions` and `user_logs` as well
-- MAGIC 
-- MAGIC **Query Any Data Format on Data Lake** Note that our transactions data is not in CSV format, but rather in Parquet format. With Databricks SQL, you can easily read in any file format using the exact same syntax. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW transactions_raw
USING PARQUET
OPTIONS (
  path '/mnt/adbquickstart/KKBox-Dataset-orig/transactions', 
  header True);
  
SELECT * FROM transactions_raw

-- COMMAND ----------

CREATE TEMPORARY VIEW user_logs_raw
USING CSV
OPTIONS (
  path '/mnt/adbquickstart/KKBox-Dataset-orig/user_logs', 
  header True);
  
SELECT * FROM user_logs_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Write a query below to retrieve a count of the distinct users and cities from the `members_raw` table.

-- COMMAND ----------

SELECT
--- Your code here
FROM 
--- Your code here

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
-- MAGIC All these questions can be answered with SQL against the data. Let's go through them one by one. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Are our subscribers growing or shrinking? By how much every month?
-- MAGIC The charts below indicate that we were having **steady month-over-month growth until early 2016**, but our growth rate has been **steadily declining since then**. 

-- COMMAND ----------

-- DBTITLE 1,Members By Registration Month
SELECT date_format(to_date(registration_init_time,'yyyyMMdd'),'y-MM') registration_month, 
  count(*) members
FROM members_raw
GROUP BY registration_month 
ORDER BY registration_month ASC

-- COMMAND ----------

-- DBTITLE 1,Monthly User Growth Rate
WITH new_monthly_users AS (
  SELECT date_format(to_date(registration_init_time,'yyyyMMdd'),'y-MM') registration_month, count(*) new_members
  FROM members_raw GROUP BY registration_month)
SELECT registration_month, new_members/(sum(new_members) OVER (ORDER BY registration_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS growth
FROM new_monthly_users
ORDER BY registration_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What is the breakdown of *new* (recently subscribed) vs. *old* (subscribed 1 or more years ago) customers?
-- MAGIC Most of our current subscribers became customers in 2016, with 2013-2015 close behind. 

-- COMMAND ----------

-- DBTITLE 1,Members by Registration Year
SELECT year(to_date(registration_init_time,'yyyyMMdd')) registration_year,
  count(*) members
FROM members_raw
GROUP BY registration_year
HAVING registration_year >= 2010

-- COMMAND ----------

-- MAGIC %md ### What % of user transactions are new subscriptions vs. renewals vs. cancellations?
-- MAGIC The chart below indicates that we have a healthy renewal and new subscriber rate, however our churn rate seems to be increasing. 

-- COMMAND ----------

-- DBTITLE 1,Breakdown of Monthly Renewals, Cancellations and New Subscriptions as a % of Transactions
WITH member_transactions AS (
  SELECT date_format(to_date(m.registration_init_time,'yyyyMMdd'),'y-MM') registration_month,
    date_format(to_date(transaction_date,'yyyyMMdd'),'y-MM') transaction_month,
    is_auto_renew, is_cancel
  FROM transactions_raw t JOIN members_raw m ON (t.msno = m.msno))
SELECT transaction_month, 
  sum(is_cancel) cancellations, 
  sum(is_auto_renew) renewals, 
  sum(CASE WHEN is_cancel = 0 AND is_auto_renew = 0 THEN 1 ELSE 0 END) new_subscriptions
FROM member_transactions
GROUP BY transaction_month
ORDER BY transaction_month ASC

-- COMMAND ----------

-- MAGIC %md ### What is the distribution of payment amount among our subscribers? Does subscriber location impact the payment amount?
-- MAGIC As shown below, the location of our subscribers seems to have an effect on the amount paid and subscription length. Perhaps we can focus our marketing efforts in higher revenue locations. 

-- COMMAND ----------

SELECT city, int(actual_amount_paid) 
FROM transactions_raw t JOIN members_raw m ON (t.msno = m.msno)

-- COMMAND ----------

-- MAGIC %md ### What is the distribution of daily listening time among our subscribers? Does subscriber location impact the listening time?
-- MAGIC Histograms are useful in understanding the distribution of a particular attribute in the data. We we below that 1) the listening time has a long tail distribution and 2) the location of the subscriber does not drasitically change the distribution.

-- COMMAND ----------

SELECT city, 
  int(total_secs) listening_time 
FROM user_logs_raw l 
  JOIN members_raw m ON (l.msno = m.msno) 
WHERE total_secs < 50000

-- COMMAND ----------

-- MAGIC %md ### Do subscribers who have been customers for a longer time use the platform more? Does it impact their likelihood to renew their subscription?
-- MAGIC Below we see that there is **more activity from younger customers** (ie. those that have been subscribers for 5 years or less), but there does **not appear to be any relationship with the listening time or subscriber days to whether a subscriber will cancel their subscription**. 

-- COMMAND ----------

WITH churned_subscribers AS (SELECT DISTINCT msno, is_cancel churned FROM transactions_raw)
SELECT l.msno, churned,
  datediff(to_date(l.date,'yyyyMMdd'),to_date(m.registration_init_time,'yyyyMMdd')) days_subscriber,
  avg(l.total_secs) daily_listen_time
FROM user_logs_raw l JOIN members_raw m ON (l.msno = m.msno)
  JOIN churned_subscribers c ON (l.msno = c.msno)
GROUP BY l.msno, days_subscriber, churned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Process and Store Transformed Data into Delta Lake
-- MAGIC Data Lakes enables the write-once, access-often analytics pattern in Azure. However, they alone do not solve the real-world challenges that come with big data processing. [Delta Lake](https://databricks.com/product/delta-lake-on-databricks) provides a layer of resiliency and performance on all data sources stored in ADLS. 
-- MAGIC 
-- MAGIC <img src="https://sguptasa.blob.core.windows.net/random/Delta%20features.png" width=800>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Based on our analysis of the KKBox data, the following transformations are needed to curate the data into Delta Lake:
-- MAGIC * The date fields need to be converted from `string` to `date` data types
-- MAGIC * The numeric fields need to be converted to `integer` data types
-- MAGIC * The flags need to be converted to `boolean` data types
-- MAGIC * Both `transactions` and `user_logs` must be enriched with `members` information

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kkbox.transactions
USING DELTA 
AS
SELECT msno,
  to_date(transaction_date, 'yyyyMMdd') transaction_date,
  int(payment_plan_days) plan_length,
  int(actual_amount_paid) plan_cost,
  to_date(membership_expire_date, 'yyyyMMdd') membership_expiry,
  CASE WHEN is_cancel=1 THEN True ELSE False END cancelled,
  CASE WHEN is_auto_renew=1 THEN True ELSE False END auto_renew
FROM transactions_raw;

SELECT * FROM kkbox.transactions

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kkbox.members
USING DELTA 
AS
WITH churned_customers AS (SELECT DISTINCT msno, cancelled churned FROM kkbox.transactions)
SELECT m.msno,
  city encoded_city, 
  to_date(registration_init_time, 'yyyyMMdd') member_registration_date,
  churned
FROM members_raw m 
  JOIN churned_customers c ON (m.msno = c.msno);

SELECT * FROM kkbox.members

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kkbox.user_logs
USING DELTA
AS
SELECT msno,
  to_date(date, 'yyyyMMdd') log_date,
  int(num_100) full_songs_listened,
  int(num_unq) unique_songs_listened,
  int(total_secs) total_listen_time
FROM user_logs_raw;

SELECT * FROM kkbox.user_logs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Our Data is Ready for Advanced Analytics!
-- MAGIC The KKBox data has been cleaned, processed and stored in a reliable and performant format on Data Lake. It can now be shared across data engineering, data science, ML, SQL analytics and reporting workloads without having to duplicate it. 
-- MAGIC 
-- MAGIC <img src="https://sguptasa.blob.core.windows.net/random/Delta%20Lakehouse.png" width=800>

-- COMMAND ----------


