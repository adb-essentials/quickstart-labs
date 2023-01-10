-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Bronze Zone

-- COMMAND ----------

CREATE LIVE VIEW date_staging
AS
select explode(sequence(to_date('2004-01-01'), to_date('2020-12-31'), interval 1 day)) as calendarDate

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE transactions_bronze
COMMENT "Parquet files from landing zone"
TBLPROPERTIES ("quality" = "landing")
AS
SELECT * 
  FROM cloud_files(
        "${Data_PATH_Ingest}/transactions*.csv", "csv", 
        map("cloudFiles.inferColumnTypes", "true",
            "header", "true"
            )
        )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE members_bronze
COMMENT "CSV files from landing zone"
TBLPROPERTIES ("quality" = "landing")
AS
SELECT * 
  FROM cloud_files(
        "${Data_PATH_Ingest}/members/", "csv", 
        map("cloudFiles.inferColumnTypes", "true",
            "header", "true"
            )
        )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE user_logs_bronze
COMMENT "CSV files from landing zone"
TBLPROPERTIES ("quality" = "landing")
AS
SELECT * 
  FROM cloud_files(
        "${Data_PATH_Ingest}/user_logs/", "csv", 
        map("cloudFiles.inferColumnTypes", "true",
            "header", "true"
            )
        )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver Zone Merge

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE members_bronze_clean_v(
  CONSTRAINT registration_init_time_nullcheck EXPECT (registration_init_time IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT msno_nullcheck EXPECT (msno IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze member view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.members_bronze);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE members_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged members";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.members_silver
FROM stream(LIVE.members_bronze_clean_v)
  KEYS (msno)
  SEQUENCE BY registration_init_time --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (_rescued_data);

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE user_logs_bronze_clean_v(
  CONSTRAINT msno_nullcheck EXPECT (msno IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze user logs view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.user_logs_bronze);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE user_logs_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged members";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.user_logs_silver
FROM stream(LIVE.user_logs_bronze_clean_v)
  KEYS (msno, date)
  SEQUENCE BY date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (_rescued_data);

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE transactions_bronze_clean_v(
  CONSTRAINT msno_nullcheck EXPECT (msno IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT transaction_date_nullcheck EXPECT (transaction_date IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze user logs view (i.e. what will become Silver)"
AS 
SELECT 
 msno,
 payment_method_id,
 payment_plan_days,
 plan_list_price,
 actual_amount_paid,
 is_auto_renew,
 to_date(CAST(transaction_date AS STRING), "yyyyMMdd") AS transaction_date,
 to_date(CAST(membership_expire_date AS STRING), "yyyyMMdd") AS membership_expire_date,
 is_cancel
FROM STREAM(LIVE.transactions_bronze);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE transactions_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged members";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.transactions_silver
FROM stream(LIVE.transactions_bronze_clean_v)
  KEYS (msno, transaction_date, membership_expire_date)
  SEQUENCE BY transaction_date --auto-incremental ID to identity order of events
; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold Zone - Star Schema for Power BI

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_member
(members_sk BIGINT GENERATED ALWAYS AS IDENTITY, 
 member_num_nk STRING,
 city_cd INT,
 age INT,
 gender STRING,
 registered_via INT,
 registration_date Date,
CONSTRAINT registration_date_nullcheck EXPECT (registration_date IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Members dimensions"
TBLPROPERTIES ("quality" = "gold", "delta.targetFileSize" = "32mb", "pipelines.autoOptimize.zOrderCols" = "members_sk,member_num_nk,registration_date")
AS
SELECT 
 msno AS member_num_nk,
 city AS city_cd,
 bd AS age,
 CASE WHEN ISNULL(gender) THEN "" ELSE gender END AS gender,
 registered_via,
 to_date(CAST(registration_init_time AS STRING), "yyyyMMdd") AS registration_date
FROM live.members_silver

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_date
COMMENT "Date dimensions"
TBLPROPERTIES ("quality" = "gold", "delta.targetFileSize" = "32mb", "pipelines.autoOptimize.zOrderCols" = "DateInt,Date")
AS 
SELECT 
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateInt,
  calendarDate AS `Date`,
  day(calendarDate) AS DayofMonth,
  month(calendarDate) as MonthNumber,
  date_format(calendarDate, 'MMMM') as Month,
  year(calendarDate) * 100 + month(calendarDate) as YearMonthInt,
  year(calendarDate) AS Year,
  dayofweek(calendarDate) AS DayOfWeekNumber,
  date_format(calendarDate, 'EEEE') as DayofWeek
FROM live.date_staging

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_transactions (
  CONSTRAINT members_fk_isnull EXPECT (members_fk IS NOT NULL AND members_fk > 0)
)
COMMENT "transactions fact"
TBLPROPERTIES ("quality" = "gold", "delta.targetFileSize" = "32mb", "pipelines.autoOptimize.zOrderCols" = "transaction_date_fk,members_fk")
AS 
SELECT 
CAST(date_format(t.transaction_date, "yyyyMMdd") AS INT) AS transaction_date_fk,
CAST(date_format(t.membership_expire_date, "yyyyMMdd") AS INT) AS membership_expire_date_fk,
CASE WHEN ISNULL(m.members_sk) THEN 0 ELSE m.members_sk END as members_fk,
t.msno,
t.payment_method_id,
CAST(t.payment_plan_days AS INT) AS payment_plan_days,
CAST(t.plan_list_price AS DOUBLE) AS plan_list_price,
t.plan_list_price - t.actual_amount_paid AS discount_applied,
CAST(t.actual_amount_paid AS DOUBLE) AS actual_amount_paid,
CAST(t.is_auto_renew AS BOOLEAN) AS is_auto_renew_flag,
CAST(t.is_cancel AS BOOLEAN) AS is_cancel_flag,
CAST(t.is_auto_renew AS INT) AS is_auto_renew_count,
CAST(t.is_cancel AS INT) AS is_cancel_count
FROM 
live.transactions_silver t
LEFT OUTER JOIN live.dim_member m ON t.msno = m.member_num_nk

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_user_logs (
  CONSTRAINT members_fk_isnull EXPECT (members_fk IS NOT NULL AND members_fk > 0)
)
COMMENT "user logs fact"
TBLPROPERTIES ("quality" = "gold", "delta.targetFileSize" = "32mb", "pipelines.autoOptimize.zOrderCols" = "transaction_date_fk,members_fk")
AS 
SELECT 
t.date AS transaction_date_fk,
CASE WHEN ISNULL(m.members_sk) THEN 0 ELSE m.members_sk END as members_fk,
t.msno,
t.num_25  AS Num_Songs_Less_25_Percent_SongLength,
t.num_50  AS Num_Songs_Less_50_Percent_SongLength,
t.num_75  AS Num_Songs_Less_75_Percent_SongLength,
t.num_985 AS Num_Songs_Less_98_Percent_SongLength,
t.num_100 AS Num_Songs_to_100_Percent_SongLength,
t.num_unq AS Unique_Songs_Played,
t.total_secs AS Total_Seconds_Played
FROM 
live.user_logs_silver t
LEFT OUTER JOIN live.dim_member m ON t.msno = m.member_num_nk
