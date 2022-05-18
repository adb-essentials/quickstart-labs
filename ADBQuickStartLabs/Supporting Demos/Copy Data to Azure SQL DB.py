# Databricks notebook source
## Read the Bronze Data
transactions_bronze = spark.read.format("delta").load('/mnt/adbquickstart/bronze/transactions/')
members_bronze = spark.read.format("delta").load('/mnt/adbquickstart/bronze/members/')
user_logs_bronze = spark.read.format("delta").load('/mnt/adbquickstart/bronze/user_logs/')

# COMMAND ----------

user = ""
pwd = ""

# Change to your own JDBC URL accordingly especially the server address and port
jdbc_url = "jdbc:sqlserver://servername.database.windows.net:1433;database=ne-sqldb;"


# COMMAND ----------

# Change to your own SQL select statement
sql = "transactions"

transactions_bronze.write \
    .format('jdbc') \
    .mode('overwrite') \
    .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver') \
    .option('url',jdbc_url) \
    .option('dbtable', '{sql}'.format(sql=sql)) \
    .option('user','{user}'.format(user=user)) \
    .option('password', pwd) \
    .save()

# COMMAND ----------

# Change to your own SQL select statement
sql = "user_logs"

user_logs_bronze.write \
    .format('jdbc') \
    .mode('overwrite') \
    .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver') \
    .option('url',jdbc_url) \
    .option('dbtable', '{sql}'.format(sql=sql)) \
    .option('user','{user}'.format(user=user)) \
    .option('password', pwd) \
    .save()

# COMMAND ----------

# Change to your own SQL select statement
sql = "members"

members_bronze.write \
    .format('jdbc') \
    .mode('overwrite') \
    .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver') \
    .option('url',jdbc_url) \
    .option('dbtable', '{sql}'.format(sql=sql)) \
    .option('user','{user}'.format(user=user)) \
    .option('password', pwd) \
    .save()

# COMMAND ----------

# Change to your own SQL select statement
sql = "transactions"

df = spark.read \
    .format('jdbc') \
    .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver') \
    .option('url',jdbc_url) \
    .option('dbtable', '{sql}'.format(sql=sql)) \
    .option('user','{user}'.format(user=user)) \
    .option('password', pwd) \
    .load()

# df.createOrReplaceTempView("tbl")
display(df)
