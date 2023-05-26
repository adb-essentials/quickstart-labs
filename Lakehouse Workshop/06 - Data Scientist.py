# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Lakehouse Labs for Data Scientist
# MAGIC Welcome to the Lakehouse lab for data scientists on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
# MAGIC 1. Access your enterprise data lake in Azure using Databricks
# MAGIC 2. Develop Machine Learning Model using Auto ML  
# MAGIC 3. Use MLFlow for end-to-end model management and lifecycle
# MAGIC
# MAGIC #### The Use Case
# MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to **create a ML model that trys to predict users that might churn from the music streaming service**. 

# COMMAND ----------

# DBTITLE 1,Lakehouse Workshop Storage Variables
# MAGIC %run "../Lakehouse Workshop/00 - Set Lab Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Widgets
# MAGIC The following notebook widgets are being created automatically for you and defaulted to your set variables for more easier parameterization of code.  

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("UserDB", UserDB)

# COMMAND ----------

dbutils.widgets.text("Data_PATH_User", Data_PATH_User)

# COMMAND ----------

dbutils.widgets.text("Data_PATH_Ingest", Data_PATH_Ingest)

# COMMAND ----------

# DBTITLE 1,Install Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.feature_store import *
from pyspark.ml.feature import *
from databricks import automl
import mlflow

# COMMAND ----------

# DBTITLE 0,ML Architecture
# MAGIC %md
# MAGIC <!-- <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/DS.png" width="1200"> -->
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/DatabricksML.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Machine Learning is a data native solution that enables data scientists to do end-to-end ML/DS in one single platform without moving data or code to different platforms.
# MAGIC
# MAGIC The journey of a data science project starts from accessing the data, understanding the data and then moving on to steps such as feature engineering, feature store creation/maintenance, model creation, model management and finally model serving. Using Databricks one can accomplish all the steps at one place.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get, Prepare, Enhance and Explore Data
# MAGIC ###Persona: Data Scientists, Data Engineers

# COMMAND ----------

# DBTITLE 1,Load training data
# MAGIC %run "../Lakehouse Workshop/00 - Setup Notebooks/00 - Setup Bronze Data ML"

# COMMAND ----------

# DBTITLE 1,Load pre-loaded data
transactions = spark.read.format("delta").load(Data_PATH_User + '/bronze/transactions')
members = spark.read.format("delta").load(Data_PATH_User + "/members")
user_logs = spark.read.format("delta").load(Data_PATH_User + '/bronze/user_log/')
train = spark.read.format("delta").load(Data_PATH_User + '/bronze/train')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1a. Create Features Tables and Publish to Feature Store

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/FeatureStore.png" width="1200">

# COMMAND ----------

# DBTITLE 1,Aggregate user log data
user_logs_consolidated = user_logs.groupBy('msno').agg(count("msno").alias('no_transactions'),
                                 sum('num_25').alias('Total25'),sum('num_100').alias('Total100'), mean('num_unq').alias('UniqueSongs'),mean('total_secs').alias('TotalSecHeard')
                               )

display(user_logs_consolidated)

# COMMAND ----------

# DBTITLE 1,Create member feature table
#The user_log data
member_feature = user_logs_consolidated.join(members, "msno")
#remove Age Oultlier. If age is greater than 100 or less than 15 we remove it
member_feature = member_feature.where("bd between 15 and 100")

#fill NA for gender not present
colNames = ["gender"]
member_feature = member_feature.na.fill("NA", colNames)

#Handle gender categorical variable:
gender_index=StringIndexer().setInputCol("gender").setOutputCol("gender_indexed")
member_feature=gender_index.fit(member_feature).transform(member_feature)

member_feature.write.format('delta').mode('overwrite').option('mergeSchema','true').save(Data_PATH_User + '/silver/member_feature')

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE IF NOT EXISTS {0}.member_features
  USING DELTA 
  LOCATION "{1}/silver/member_feature"
  '''.format(UserDB, Data_PATH_User))

# COMMAND ----------

# DBTITLE 1,Register member feature table
fs = FeatureStoreClient()

fs.register_table(
  delta_table= UserDB + '.member_features',
  primary_keys='msno',
  description='Member features commonly used in ML model'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Browse available Feature Tables in the Databricks Feature Store
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/FeatureStoreSearch.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review features and their metadata.  See upstream and downstream feature lineage along with feature freshness
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/FeatureStoreUI.1.png" width="1200">
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/FeatureStoreUI2.1.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Create and cleanse inference data + Feature Engineering

# COMMAND ----------

# DBTITLE 1,Merge datasets 
data = transactions.join(train,"msno").join(members, "msno")
columns_to_drop = ['city', 'bd', 'registered_via' ]
data = data.drop(*columns_to_drop)
display(data)

# COMMAND ----------

# DBTITLE 1,Inference Data Cleaning and Feature Engineering
# Create a Feature Days a userhas been on platform
churn_data =  data.withColumn("DaysOnBoard",datediff(to_date(data['membership_expire_date'], 'yyyyMMdd'),(to_date(data['registration_init_time'], 'yyyyMMdd'))))
#Find out if there was a discount provided to the user
churn_data = churn_data.withColumn("Discount", churn_data['actual_amount_paid']-churn_data['plan_list_price'])
#churn_data.where("Discount > 0").show()

#dropping unrequired columns: 
columns_to_drop = ['membership_expire_date', 'registration_init_time', 'actual_amount_paid', 'plan_list_price', 'transaction_date' ]
churn_data = churn_data.drop(*columns_to_drop)

colNames = ["gender"]
churn_data = churn_data.na.fill("NA", colNames)

churn_data.write.format('delta').mode('overwrite').option("mergeSchema","true").save(Data_PATH_User + '/silver/churndata')

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE IF NOT EXISTS {0}.churndata
  USING DELTA 
  LOCATION '{1}/silver/churndata'
  '''.format(UserDB, Data_PATH_User))

# COMMAND ----------

# MAGIC %md
# MAGIC Explore Churn Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${UserDB}.churndata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1c. Create training dataset and lookup features from Feature Store.  Write to Delta table

# COMMAND ----------

# The model training uses two features from the 'customer_features' feature table and
# a single feature from 'product_features'
feature_lookups = [
    FeatureLookup(
      table_name = UserDB + '.member_features',
      feature_names = ['no_transactions','Total25','Total100','UniqueSongs','TotalSecHeard','city','bd','registered_via','registration_init_time','gender_indexed'],
      lookup_key = 'msno'
    )
  ]

fs = FeatureStoreClient()

training_set = fs.create_training_set(
  df=spark.read.table(UserDB + ".churndata"),
  feature_lookups = feature_lookups,
  label = 'is_churn',
  exclude_columns = ['msno']
)

training_df = training_set.load_df().where("registration_init_time is not null")

training_df.write.format('delta').mode('overwrite').option('mergeSchema','true').save(Data_PATH_User + '/silver/trainingdata')

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE IF NOT EXISTS {0}.trainingdata
  USING DELTA 
  LOCATION '{1}/silver/trainingdata'
  '''.format(UserDB, Data_PATH_User))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${UserDB}.trainingdata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1d. Create training and testing dataframe

# COMMAND ----------

trainDF, testDF = spark.table(UserDB + '.trainingdata').randomSplit([.8, .2], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. AutoML
# MAGIC ###Persona: Data Scientists

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/AutoML.png" width="1200">

# COMMAND ----------

summary = automl.classify(
  trainDF, 
  target_col="is_churn", 
  primary_metric="f1", 
  timeout_minutes=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure Databricks AutoML runs can also be kicked off using the workspace UI
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/AutoML.png" width="1200">
# MAGIC
# MAGIC ##Advanced options UI 
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/AutoML2.png" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Model registry: CMI/CMD: Continuous Model Integration & Continuous Model Deployment
# MAGIC ### Persona: Model Validation and Governance Team  
# MAGIC All data scientists can then register their best models to a common registry

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/MLflow.png" width="1400">

# COMMAND ----------

# MAGIC %md #### Register the model with the MLflow Model Registry
# MAGIC
# MAGIC Now that a ML model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI (Workflow 1) or the MLflow API (Workflow 2).
# MAGIC
# MAGIC Follow the instructions for your preferred workflow (UI or API) to register your forecasting model, add rich model descriptions, and perform stage transitions.  
# MAGIC
# MAGIC You can open the notebook outline and skip to your preferred section....  
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/MLFlowWorkflows.png" width="200">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflow 1- Register Model via UI

# COMMAND ----------

# DBTITLE 1,Navigate to AutoML Experiments
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML0.png" width="800">

# COMMAND ----------

# DBTITLE 1,Let's Click the Experiment and see all the runs we have for the model
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML1.png" width="1400">

# COMMAND ----------

# DBTITLE 1,When you click "model" under artifacts click "Register Model" to register the particular model
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML2.png" width="1400">

# COMMAND ----------

# DBTITLE 1,Give the model a name
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML3.png" width="600">

# COMMAND ----------

# DBTITLE 1,Click "Models on the left Nav to get the list of all Registered model. Click the Model we just saved
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML4.png" width="1400">

# COMMAND ----------

# DBTITLE 1,You can see the various versions of the model. Click the version you want to move into production
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML5.png" width="1200">

# COMMAND ----------

# DBTITLE 1,Click "Stage" to move model through various stages
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML6.png" width="1200">

# COMMAND ----------

# DBTITLE 1,Moving the Model to production
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML7.png" width="600">

# COMMAND ----------

# DBTITLE 1,Once Done come back and check the models page to see the model has been moved to production !!!
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/ML8.png" width="1400">

# COMMAND ----------

# MAGIC %md
# MAGIC ###Workflow 2 - Register model via the API

# COMMAND ----------

#get the best trial from our AutoML run programmatically
print(summary.best_trial)

# COMMAND ----------

# Register the best model run
model_name = "KKBox-Churn-Prediction-" + UserDB

model_uri = f"runs:/{summary.best_trial.mlflow_run_id}/model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DBTITLE 1,Add Model Description
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_registered_model(
  name=registered_model_version.name,
  description="This model predicts churn of KKbox customers using an AutoML model."
)

# COMMAND ----------

# MAGIC %md ### Perform a model stage transition
# MAGIC
# MAGIC The MLflow Model Registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC
# MAGIC Users with appropriate permissions can transition models between stages. Your administrators in your organization will be able to control these permissions on a per-user and per-model basis.
# MAGIC
# MAGIC If you have permission to transition a model to a particular stage, you can make the transition directly by using the `MlflowClient.update_model_version()` function. If you do not have permission, you can request a stage transition using the REST API; for example:
# MAGIC
# MAGIC ```
# MAGIC %sh curl -i -X POST -H "X-Databricks-Org-Id: <YOUR_ORG_ID>" -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" https://<YOUR_DATABRICKS_WORKSPACE_URL>/api/2.0/preview/mlflow/transition-requests/create -d '{"comment": "Please move this model into production!", "model_version": {"version": 1, "registered_model": {"name": "power-forecasting-model"}}, "stage": "Production"}'

# COMMAND ----------

# MAGIC %md Now that you've learned about stage transitions, transition the model to the `Production` stage.

# COMMAND ----------

# MAGIC %python
# MAGIC client.transition_model_version_stage(
# MAGIC   name=model_name,
# MAGIC   version=registered_model_version.version,
# MAGIC   stage='Production',
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC ##4.Use Production Model in a Downstream application
# MAGIC ####Persona: Model Deployment Team

# COMMAND ----------

# MAGIC %md ### Model Serving
# MAGIC Now that the model is in Production we are ready for our next step - Model Serving
# MAGIC For this workshop we will serve the model in two ways:
# MAGIC 1. Use Production Model in a Downstream application - Batch Inference
# MAGIC 2. MLflow Model Serving on Databricks - Classic and Serverless
# MAGIC 3. AKS and AML

# COMMAND ----------

# MAGIC %md
# MAGIC Batch Inference Testing

# COMMAND ----------


model_name = registered_model_version.name
model_version = registered_model_version.version

# model_name = "KKBox-Churn-Prediction"
# model_version = 1

model_uri=f"models:/{model_name}/{model_version}"

# Create a python function using the model_uri
# score the testDF dataframe using the python function
# Batch scoring in Databricks is that easy and scalable
predict = mlflow.pyfunc.spark_udf(spark, model_uri)
predDF = testDF.withColumn("prediction", predict(*testDF.drop("is_churn").columns))
display(predDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Batch Inference Testing Accuracy

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# How accurate was our model against the testDF dataframe?
evaluator = MulticlassClassificationEvaluator(labelCol="is_churn", predictionCol="prediction")
f1 = evaluator.setMetricName("f1").evaluate(predDF)
print(f"f1 on test dataset: {f1}")

# COMMAND ----------

# MAGIC %md
# MAGIC Write Predictions to a Gold table

# COMMAND ----------

goldDF = spark.table(UserDB + '.trainingdata')
goldDF = goldDF.withColumn("prediction", predict(*goldDF.drop("is_churn").columns))

goldDF.write.format('delta').mode('overwrite').option('mergeSchema','true').save(Data_PATH_User + '/gold/scoreddata')

# create table object to make delta lake queriable
spark.sql('''
  CREATE TABLE IF NOT EXISTS {0}.scoreddata
  USING DELTA 
  LOCATION '{1}/gold/scoreddata'
  '''.format(UserDB, Data_PATH_User))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${UserDB}.scoreddata

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Model Serving on Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC Navigate to the model in the model registry
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Serving0.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC Enable MLflow Model Serving from UI
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Serving1.png" width="1400">
# MAGIC
# MAGIC Depending on your workspace configuration, you may see serverless model serving as your main option   
# MAGIC <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/serverless-compute/serverless-models-enable-serving-pane.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC Select MLflow Model Serving compute size and click Save
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Serving2.png" width="800">

# COMMAND ----------


import os
import requests
import numpy as np
import pandas as pd

# ModelName = "KKBox-Churn-Prediction"
# Version = "2"

ModelName = model_name
Version = model_version

url = f'https://{Workspace}/model/{ModelName}/{Version}/invocations'

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = f'https://{Workspace}/model/{ModelName}/{Version}/invocations'
  headers = {'Authorization': f'Bearer {Databricks_Token}'}
  data_json = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

# Generate some testing data to send to the model REST API
new_data_pandas = testDF.drop("is_churn").limit(5).withColumn("registration_init_time", testDF.registration_init_time.cast(StringType())).toPandas()
new_data_pandas

# COMMAND ----------

# Score the data via the MLFlow model REST API
# Model serving is designed for low-latency predictions on smaller batches of data
served_predictions = score_model(new_data_pandas)
pd.DataFrame({
  "Served Model Prediction": served_predictions,
})

# COMMAND ----------

# MAGIC %md
# MAGIC Turn off MLflow Model Serving compute
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/adb-essentials/quickstart-labs/main/images/Serving3.png" width="800">
