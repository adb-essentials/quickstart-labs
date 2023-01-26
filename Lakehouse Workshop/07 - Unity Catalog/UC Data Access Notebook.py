# Databricks notebook source
# MAGIC %md
# MAGIC ### Table Access

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG uc_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE uc_demo_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uc_demo.information_schema.tables WHERE table_schema='uc_demo_db'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uc_demo.uc_demo_db.members

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE uc_demo.uc_demo_db.members SET city=10  WHERE msno='WFLY3s7z4EZsieHCt63XrsdtfTEmJ+2PnnKLH5GY4Tk='

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uc_demo.uc_demo_db.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uc_demo.uc_demo_db.user_logs_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### File Access

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST 'abfss://datalake@lafadlspltest.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST 'abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db'

# COMMAND ----------

# MAGIC %sql
# MAGIC list 'abfss://kkbox@lafadlspltest.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %md
# MAGIC Read files into DF

# COMMAND ----------

df = spark.read.format("delta").load("abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs_summary")
display(df)

# COMMAND ----------

df_fo = spark.read.format("delta").load("abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs")
display(df_fo)

# COMMAND ----------

df_fo.write.format("delta").save("abfss://datalake@lafadlspltest.dfs.core.windows.net/uc_demo/uc_demo_db/user_logs_two")

# COMMAND ----------

df_fo.write.format("delta").saveAsTable("uc_demo.uc_demo_db.user_logs_2")
