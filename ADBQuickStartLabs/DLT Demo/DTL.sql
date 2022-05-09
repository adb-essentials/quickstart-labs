-- Databricks notebook source
CREATE LIVE VIEW date_staging
AS
select explode(sequence(to_date('2019-01-01'), to_date('2020-12-31'), interval 1 day)) as calendarDate

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE members_bronze
COMMENT "CSV files from landing zone"
TBLPROPERTIES ("quality" = "landing")
AS
SELECT * 
  FROM cloud_files(
        "/mnt/adbquickstart/members/", "csv", 
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
        "/mnt/adbquickstart/user_logs/", "csv", 
        map("cloudFiles.inferColumnTypes", "true",
            "header", "true"
            )
        )

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
  KEYS (msno)
  SEQUENCE BY date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (_rescued_data);

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_member
(members_sk BIGINT GENERATED ALWAYS AS IDENTITY, 
 member_nm_nk STRING,
 city_cd INT,
 age INT,
 gender STRING,
 registered_via INT,
 registration_date Date,
CONSTRAINT registration_date_nullcheck EXPECT (registration_date IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Members dimensions"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
 msno AS member_nm_nk,
 city AS city_cd,
 bd AS age,
 CASE WHEN ISNULL(gender) THEN "" ELSE gender END AS gender,
 registered_via,
 to_date(CAST(registration_init_time AS STRING), "yyyyMMdd") AS registration_date
FROM live.members_silver

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_date
COMMENT "Date dimensions"
TBLPROPERTIES ("quality" = "gold")
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



-- COMMAND ----------

-- SELECT * FROM dlt_leo.transactions_landing 

-- COMMAND ----------

-- DROP DATABASE dlt_leo CASCADE

-- COMMAND ----------

-- SELECT COUNT(1) FROM dlt_leo.members_bronze--6,769,473

-- SELECT COUNT(DISTINCT MSNO) FROM dlt_leo.members_bronze--6,769,473

-- COMMAND ----------

-- SELECT * FROM dlt_leo.user_logs_landing
