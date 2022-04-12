# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Quickstart for Data Scientist
# MAGIC Welcome to the quickstart lab for data analysts on Azure Databricks! Over the course of this notebook, you will use a real-world dataset and learn how to:
# MAGIC 1. Access your enterprise data lake in Azure using Databricks
# MAGIC 2. Develop Machine Learning Model 
# MAGIC 3. Use MLFlow for end-to-end model management and lifecycle
# MAGIC 
# MAGIC #### The Use Case
# MAGIC We will analyze public subscriber data from a popular Korean music streaming service called KKbox stored in Azure Blob Storage. The goal of the notebook is to answer a set of business-related questions about our business, subscribers and usage. 

# COMMAND ----------

# DBTITLE 0,ML Architecture
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/DS.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Unified Analytics and Data Platform enables dats scientists to do end-to-end ML/DS at one single place without moving data or code to different platforms.
# MAGIC 
# MAGIC ###The journey of a data science project starts from accessing the data, understanding the data and then moving on to steps such as feature engineering, model creation, model management and finally model serving. Using Databricks one can accomplish all the steps at one place.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount("/mnt/adbquickstart")

# COMMAND ----------

# DBTITLE 1,Enter Variable Values
# MAGIC %python
# MAGIC import os
# MAGIC BLOB_CONTAINER = "channelsa-datasets"
# MAGIC BLOB_ACCOUNT = "channelsapublicprodblob"
# MAGIC ACCOUNT_KEY = "iIVYXXRC+F9Eh/sc80R0bhhDd12l52JTc/wsZfx9bcMazLejX5dXDaqNoErLZqOQprsaLEjM8YMlRrQuCZonJQ=="
# MAGIC os.environ["DATABRICKS_TOKEN"] = "dapiff2665241cefbb990fccb70f979f2350-3"

# COMMAND ----------

# DBTITLE 1,Load libraries
# MAGIC %python
# MAGIC #Import the necessary libraries
# MAGIC import shutil
# MAGIC import mlflow
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.mllib.stat import Statistics
# MAGIC from pyspark.ml.stat import ChiSquareTest
# MAGIC from pyspark.sql import functions
# MAGIC from pyspark.sql.functions import isnan, when, count, col
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import matplotlib.pyplot as mplt
# MAGIC import matplotlib.ticker as mtick
# MAGIC from pyspark.ml.regression import GeneralizedLinearRegression,RandomForestRegressor
# MAGIC from pyspark.ml import Pipeline
# MAGIC from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
# MAGIC from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, VectorIndexer,StandardScaler,IndexToString
# MAGIC from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator, MulticlassClassificationEvaluator
# MAGIC from pyspark.ml import Pipeline

# COMMAND ----------

# DBTITLE 1,Drop Database if needed
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS kkbox CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get, Prepare, Enhance and Explore Data
# MAGIC ###Persona: Data Scientists, Data Engineers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Data to a Notebook
# MAGIC ####Mounting Azure Storage using an Access Key or Service Principal
# MAGIC We will mount an Azure blob storage container to the workspace using a shared Access Key. More instructions can be found [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage#--mount-azure-blob-storage-containers-to-dbfs). 

# COMMAND ----------

# DBTITLE 1,Mount Blob onto DBFS (Run this cell only once)
# MAGIC %python
# MAGIC DIRECTORY = "/"
# MAGIC MOUNT_PATH = "/mnt/adbquickstart"
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT}.blob.core.windows.net/ADB-Quickstart/KKBox-Dataset",
# MAGIC   mount_point = MOUNT_PATH,
# MAGIC   extra_configs = {
# MAGIC     f"fs.azure.account.key.{BLOB_ACCOUNT}.blob.core.windows.net":ACCOUNT_KEY
# MAGIC   }
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC # drop any old delta lake files that might have been created
# MAGIC shutil.rmtree('/dbfs/mnt/adbquickstart/bronze', ignore_errors=True)
# MAGIC shutil.rmtree('/dbfs/mnt/adbquickstart/gold', ignore_errors=True)
# MAGIC shutil.rmtree('/dbfs/mnt/adbquickstart/silver', ignore_errors=True)
# MAGIC shutil.rmtree('/dbfs/mnt/adbquickstart/checkpoint', ignore_errors=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Once mounted, we can view and navigate the contents of our container using Databricks `%fs` file system commands.

# COMMAND ----------

# MAGIC %fs ls /mnt/adbquickstart/

# COMMAND ----------

# MAGIC %sql
# MAGIC create database kkbox

# COMMAND ----------

# DBTITLE 1,Get Transaction dataset
# MAGIC %python
# MAGIC # transaction dataset schema
# MAGIC transaction_schema = StructType([
# MAGIC   StructField('msno', StringType()),
# MAGIC   StructField('payment_method_id', IntegerType()),
# MAGIC   StructField('payment_plan_days', IntegerType()),
# MAGIC   StructField('plan_list_price', IntegerType()),
# MAGIC   StructField('actual_amount_paid', IntegerType()),
# MAGIC   StructField('is_auto_renew', IntegerType()),
# MAGIC   StructField('transaction_date', DateType()),
# MAGIC   StructField('membership_expire_date', DateType()),
# MAGIC   StructField('is_cancel', IntegerType())  
# MAGIC   ])
# MAGIC 
# MAGIC # read data from parquet
# MAGIC transactions = (
# MAGIC   spark
# MAGIC     .read
# MAGIC     .csv(
# MAGIC       '/mnt/adbquickstart/transactions/transactions_v2.csv',
# MAGIC       schema=transaction_schema,
# MAGIC       header=True,
# MAGIC       dateFormat='yyyyMMdd'
# MAGIC       )
# MAGIC     )
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # persist in delta lake format
# MAGIC ( transactions
# MAGIC     .write
# MAGIC     .format('delta')
# MAGIC     .partitionBy('transaction_date')
# MAGIC     .mode('overwrite')
# MAGIC     .save('/mnt/adbquickstart/bronze/transactions')
# MAGIC   )
# MAGIC 
# MAGIC # create table object to make delta lake queriable
# MAGIC spark.sql('''
# MAGIC   CREATE TABLE kkbox.transactions
# MAGIC   USING DELTA 
# MAGIC   LOCATION '/mnt/adbquickstart/bronze/transactions'
# MAGIC   ''')

# COMMAND ----------

# DBTITLE 1,Get members dataset
# MAGIC %python
# MAGIC # members dataset schema
# MAGIC member_schema = StructType([
# MAGIC   StructField('msno', StringType()),
# MAGIC   StructField('city', IntegerType()),
# MAGIC   StructField('bd', IntegerType()),
# MAGIC   StructField('gender', StringType()),
# MAGIC   StructField('registered_via', IntegerType()),
# MAGIC   StructField('registration_init_time', DateType())
# MAGIC   ])
# MAGIC 
# MAGIC # read data from csv
# MAGIC members = (
# MAGIC   spark
# MAGIC     .read
# MAGIC     .csv(
# MAGIC       'dbfs:/mnt/adbquickstart/members/members_v3.csv',
# MAGIC       schema=member_schema,
# MAGIC       header=True,
# MAGIC       dateFormat='yyyyMMdd'
# MAGIC       )
# MAGIC     )
# MAGIC 
# MAGIC # persist in delta lake format
# MAGIC (
# MAGIC   members
# MAGIC     .write
# MAGIC     .format('delta')
# MAGIC     .mode('overwrite')
# MAGIC     .save('/mnt/adbquickstart/bronze/members')
# MAGIC   )
# MAGIC 
# MAGIC # create table object to make delta lake queriable
# MAGIC spark.sql('''
# MAGIC   CREATE TABLE kkbox.members 
# MAGIC   USING DELTA 
# MAGIC   LOCATION '/mnt/adbquickstart/bronze/members'
# MAGIC   ''')

# COMMAND ----------

# DBTITLE 1,Get user log data
# MAGIC %python
# MAGIC 
# MAGIC log_schema = StructType([
# MAGIC   StructField('msno', StringType()),
# MAGIC   StructField('date', StringType()),
# MAGIC   StructField('num_25', IntegerType()),
# MAGIC   StructField('num_50', StringType()),
# MAGIC   StructField('num_75', IntegerType()),
# MAGIC   StructField('num_985', IntegerType()),
# MAGIC   StructField('num_100', IntegerType()),
# MAGIC   StructField('num_unq', IntegerType()),
# MAGIC   StructField('total_secs', DoubleType())
# MAGIC   ])
# MAGIC 
# MAGIC # read data from csv
# MAGIC user_log = (
# MAGIC   spark
# MAGIC     .read
# MAGIC     .csv(
# MAGIC       'dbfs:/mnt/adbquickstart/user_logs/user_logs_v2.csv',
# MAGIC       schema=log_schema,
# MAGIC       header=True,
# MAGIC       dateFormat='yyyyMMdd'
# MAGIC       )
# MAGIC     )
# MAGIC # persist in delta lake format
# MAGIC (
# MAGIC   user_log
# MAGIC     .write
# MAGIC     .format('delta')
# MAGIC     .mode('overwrite')
# MAGIC     .save('/mnt/adbquickstart/bronze/user_log')
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Get churn dataset
# MAGIC %python
# MAGIC # train dataset schema
# MAGIC train_schema = StructType([
# MAGIC   StructField('msno', StringType()),
# MAGIC   StructField('is_churn', IntegerType())
# MAGIC   ])
# MAGIC 
# MAGIC # read data from csv
# MAGIC train = (
# MAGIC   spark
# MAGIC     .read
# MAGIC     .csv(
# MAGIC       'dbfs:/mnt/adbquickstart/train_v2.csv',
# MAGIC       schema=train_schema,
# MAGIC       header=True
# MAGIC       )
# MAGIC     )
# MAGIC 
# MAGIC # persist in delta lake format
# MAGIC (
# MAGIC   train
# MAGIC     .write
# MAGIC     .format('delta')
# MAGIC     .mode('overwrite')
# MAGIC     .save('/mnt/adbquickstart/bronze/train')
# MAGIC   )

# COMMAND ----------

# MAGIC %python
# MAGIC transactions = spark.read.format("delta").load('/mnt/adbquickstart/bronze/transactions/')
# MAGIC members = spark.read.format("delta").load('/mnt/adbquickstart/bronze/members/')
# MAGIC user_logs = spark.read.format("delta").load('/mnt/adbquickstart/bronze/user_log/')
# MAGIC train= spark.read.format("delta").load('/mnt/adbquickstart/bronze/train/')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Enrich the data to get additional insights

# COMMAND ----------

# DBTITLE 1,Aggregate user log data
# MAGIC %python
# MAGIC #The user_log data is 
# MAGIC user_logs_consolidated =user_logs.groupBy('msno').agg(count("msno").alias('no_transactions'),
# MAGIC                                  sum('num_25').alias('Total25'),sum('num_100').alias('Total100'), mean('num_unq').alias('UniqueSongs'),mean('total_secs').alias('TotalSecHeard')
# MAGIC                                )
# MAGIC 
# MAGIC user_logs_consolidated.show()

# COMMAND ----------

# DBTITLE 1,Merge datasets 
# MAGIC %python
# MAGIC data = user_logs_consolidated.join(transactions,"msno").join(train,"msno").join(members, "msno")
# MAGIC data.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table churndata 

# COMMAND ----------

# DBTITLE 1,Data Cleaning and Feature Engineering
# MAGIC %python
# MAGIC #remove Age Oultlier. If age is greater than 100 or less than 15 we remove it
# MAGIC churn_data = data.where("bd between 15 and 100")
# MAGIC 
# MAGIC #fill NA for gender not present
# MAGIC colNames = ["gender"]
# MAGIC churn_data = churn_data.na.fill("NA", colNames)
# MAGIC 
# MAGIC #Handle gender categorical variable:
# MAGIC gender_index=StringIndexer().setInputCol("gender").setOutputCol("gender_indexed")
# MAGIC churn_data=gender_index.fit(churn_data).transform(churn_data)
# MAGIC 
# MAGIC # Create a Feature Days a userhas been on platform
# MAGIC churn_data =  churn_data.withColumn("DaysOnBoard",datediff(churn_data['membership_expire_date'],churn_data['registration_init_time']))
# MAGIC #Find out if there was a discount provided to the user
# MAGIC churn_data = churn_data.withColumn("Discount", churn_data['actual_amount_paid']-churn_data['plan_list_price'])
# MAGIC #churn_data.where("Discount > 0").show()
# MAGIC 
# MAGIC #dropping unrequired columns: 
# MAGIC columns_to_drop = ['membership_expire_date', 'registration_init_time', 'actual_amount_paid', 'plan_list_price','transaction_date' ]
# MAGIC churn_data = churn_data.drop(*columns_to_drop)
# MAGIC 
# MAGIC churn_data.write.saveAsTable("churndata")

# COMMAND ----------

# MAGIC %md ###Explore Churn Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from churndata

# COMMAND ----------

# DBTITLE 1,Lets see how does churn relates to gender
# MAGIC %sql
# MAGIC select  is_churn, count(is_churn) as churn , gender from churndata group by gender,is_churn

# COMMAND ----------

# DBTITLE 1,and churn w.r.t to discount
# MAGIC %sql
# MAGIC select  is_churn, count(is_churn) as churn , discount from churndata group by discount,is_churn order by discount

# COMMAND ----------

# DBTITLE 1,Initialize MLflow Settings
# MAGIC %python
# MAGIC #Set MLFlow Experiment
# MAGIC import mlflow
# MAGIC #experimentName = "/Users/odl_user_306046@databrickslabs.onmicrosoft.com/KKbox"
# MAGIC #mlflow.set_experiment(experimentName)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Train, Track and Log Models
# MAGIC ###Persona: Data Scientists

# COMMAND ----------

# MAGIC %python
# MAGIC churn_data = churn_data.drop("gender")
# MAGIC # Identify and index labels that could be fit through classification pipeline
# MAGIC labelIndexer = StringIndexer(inputCol="is_churn", outputCol="indexedLabel").fit(churn_data)
# MAGIC 
# MAGIC # Incorporate all input fields as vector for classificaion pipeline
# MAGIC assembler = VectorAssembler(inputCols=[ 'no_transactions', 'Total25', 'Total100', 'UniqueSongs', 'TotalSecHeard', 'payment_method_id', 'payment_plan_days', 'is_auto_renew',  'is_cancel', 'bd',  'registered_via', 'DaysOnBoard', 'Discount', "gender_indexed"], outputCol="features_assembler").setHandleInvalid("skip")
# MAGIC 
# MAGIC # Scale input fields using standard scale
# MAGIC scaler = StandardScaler(inputCol="features_assembler", outputCol="features")
# MAGIC 
# MAGIC # Convert/Lookup prediction label index to actual label
# MAGIC labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)

# COMMAND ----------

# DBTITLE 1,Split into Test/Train Data
# MAGIC %python
# MAGIC splits=churn_data.randomSplit([0.7 , 0.3])
# MAGIC train_data=splits[0]
# MAGIC test_data=splits[1]

# COMMAND ----------

# MAGIC %python
# MAGIC def classificationModel(stages, params, train, test):
# MAGIC   pipeline = Pipeline(stages=stages)
# MAGIC   
# MAGIC   with mlflow.start_run(run_name="KKbox-ML") as ml_run:
# MAGIC     for k,v in params.items():
# MAGIC       mlflow.log_param(k, v)
# MAGIC       
# MAGIC     mlflow.set_tag("state", "dev")
# MAGIC       
# MAGIC     model = pipeline.fit(train)
# MAGIC     predictions = model.transform(test)
# MAGIC 
# MAGIC     evaluator = MulticlassClassificationEvaluator(
# MAGIC                 labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
# MAGIC     accuracy = evaluator.evaluate(predictions)
# MAGIC     predictions.select("predictedLabel", "is_churn").groupBy("predictedLabel", "is_churn").count().toPandas().to_pickle("confusion_matrix.pkl")
# MAGIC     
# MAGIC     mlflow.log_metric("accuracy", accuracy)
# MAGIC     mlflow.log_artifact("confusion_matrix.pkl")
# MAGIC     mlflow.spark.log_model(spark_model=model, artifact_path='model') 
# MAGIC     
# MAGIC     print("Documented with MLflow Run id %s" % ml_run.info.run_uuid)
# MAGIC   
# MAGIC   return predictions, accuracy, ml_run.info

# COMMAND ----------

# MAGIC %python
# MAGIC numTreesList = [10]
# MAGIC maxDepthList = [5]
# MAGIC for numTrees, maxDepth in [(numTrees,maxDepth) for numTrees in numTreesList for maxDepth in maxDepthList]:
# MAGIC   params = {"numTrees":numTrees, "maxDepth":maxDepth, "model": "RandomForest"}
# MAGIC   rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=numTrees, maxDepth=maxDepth)

# COMMAND ----------

# MAGIC %python
# MAGIC   predictions, accuracy, ml_run_info = classificationModel([labelIndexer, assembler, scaler, rf, labelConverter], params, train_data, test_data)
# MAGIC   print("Trees: %s, Depth: %s, Accuracy: %s\n" % (numTrees, maxDepth, accuracy))

# COMMAND ----------

# MAGIC %python
# MAGIC runid = ml_run_info.run_uuid
# MAGIC mlflowclient = mlflow.tracking.MlflowClient()

# COMMAND ----------

# MAGIC %python
# MAGIC mlflowclient.get_run(runid).to_dictionary()["data"]["params"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise
# MAGIC ####Create a SKlearn Randon Forest Classifier model and log the runs in the same Experiment

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/MLflow.png" width="1400">

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Model registry: CMI/CMD: Continuous Model Integration & Continuous Model Deployment
# MAGIC ### Persona: Model Validation and Governance Team  
# MAGIC All data scientists can then register their best models to a common registry

# COMMAND ----------

# MAGIC %md #### Register the model with the MLflow Model Registry
# MAGIC 
# MAGIC Now that a ML model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI (Workflow 1) or the MLflow API (Workflow 2).
# MAGIC 
# MAGIC Follow the instructions for your preferred workflow (UI or API) to register your forecasting model, add rich model descriptions, and perform stage transitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflow 1- Register Model via UI

# COMMAND ----------

# DBTITLE 1,Let's Click the Experiment and see all the runs we have for the model
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML1.png" width="1400">

# COMMAND ----------

# DBTITLE 1,When you click "model" under artifacts click "Register Model" to register the particular model
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML2.png" width="1400">

# COMMAND ----------

# DBTITLE 1,Give model a name
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML3.png" width="1400">

# COMMAND ----------

# DBTITLE 1,Click "Models on the Right Tab to get the list of all Registered model. Click the Model we just saved
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML4.png" width="1400">

# COMMAND ----------

# DBTITLE 1,You can see the various versions of the model. Click the version you want to move into production
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML5.png" width="1200">

# COMMAND ----------

# DBTITLE 1,Click "Stage" to move model through various stages
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML6.png" width="1200">

# COMMAND ----------

# DBTITLE 1,Moving the Model to production
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML7.png" width="1000">

# COMMAND ----------

# DBTITLE 1,Once Done come back and check the models page to see the model has been moved to production !!!
# MAGIC %md
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ML8.png" width="1400">

# COMMAND ----------

# MAGIC %md
# MAGIC ###Workflow 2 - Register model via the API

# COMMAND ----------

# MAGIC %python
# MAGIC model_name = "KKBox-Churn-Prediction"

# COMMAND ----------

# MAGIC %python
# MAGIC import mlflow
# MAGIC 
# MAGIC # The default path where the MLflow autologging function stores the Keras model
# MAGIC artifact_path = "model"
# MAGIC model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=runid, artifact_path=artifact_path)
# MAGIC 
# MAGIC model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

# DBTITLE 1,Add Model Description
# MAGIC %python
# MAGIC from mlflow.tracking.client import MlflowClient
# MAGIC 
# MAGIC client = MlflowClient()
# MAGIC client.update_registered_model(
# MAGIC   name=model_details.name,
# MAGIC   description="This model predicts churn of KKbox customers using a Random Forest Classifier ."
# MAGIC )

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
# MAGIC   version=model_details.version,
# MAGIC   stage='Production',
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Use the MlflowClient.get_model_version_details() function to fetch the model's current stage.

# COMMAND ----------

# MAGIC %python
# MAGIC model_version_details = client.get_model_version(
# MAGIC   name=model_details.name,
# MAGIC   version=model_details.version,
# MAGIC )
# MAGIC print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##4.Use Production Model in a Downstream application
# MAGIC ####Persona: Model Deployment Team

# COMMAND ----------

# MAGIC %md ### Model Serving
# MAGIC Now that the model is in Production we are ready for our next step - Model Serving
# MAGIC For this workshop we will serve the model in two ways:
# MAGIC 1. Use Production Model in a Downstream application - Batch Inference
# MAGIC 2. MLflow Model Serving on Databricks (Public Preview)
# MAGIC 3. AKS and AML

# COMMAND ----------

# MAGIC %python
# MAGIC sample = train_data.limit(5)
# MAGIC display(sample)

# COMMAND ----------

# MAGIC %python
# MAGIC # To simulate a new corpus of data, save the existing X_train data to a Delta table. 
# MAGIC # In the real world, this would be a new batch of data.
# MAGIC sample = train_data.limit(5)
# MAGIC #spark_df = spark.createDataFrame(sample)
# MAGIC # Replace <username> with your username before running this cell.
# MAGIC table_path = "/mnt/adbquickstart/batch/"
# MAGIC # Delete the contents of this path in case this cell has already been run
# MAGIC dbutils.fs.rm(table_path, True)
# MAGIC sample.write.format("delta").save(table_path)
# MAGIC # Read the "new data" from Delta
# MAGIC new_data = spark.read.format("delta").load(table_path)

# COMMAND ----------

# MAGIC %python
# MAGIC def get_run_id(model_name):
# MAGIC   """Get production model id from Model Regigistry"""
# MAGIC   
# MAGIC   prod_run = [run for run in client.search_model_versions("name='KKBox-Churn-Prediction'") 
# MAGIC                   if run.current_stage == 'Production'][0]
# MAGIC   
# MAGIC   return prod_run.run_id
# MAGIC 
# MAGIC 
# MAGIC run_id = get_run_id('KKBox-Churn-Prediction')
# MAGIC 
# MAGIC # Spark flavor
# MAGIC model = mlflow.spark.load_model(f'runs:/{run_id}/{artifact_path}')
# MAGIC predictions = model.transform(new_data)

# COMMAND ----------

# MAGIC %python
# MAGIC predictions.write.mode('overwrite').format('delta').saveAsTable('default.kkbox_predictions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kkbox_predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Model Serving on Databricks (Public Preview)

# COMMAND ----------

# MAGIC %python
# MAGIC import os
# MAGIC import requests
# MAGIC import pandas as pd
# MAGIC 
# MAGIC def score_model(dataset: pd.DataFrame):
# MAGIC   url = 'https://adb-2441327760606579.19.azuredatabricks.net/model/KKBox-Churn-Prediction/1/invocations'
# MAGIC   headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
# MAGIC   data_json = dataset.to_dict(orient='split')
# MAGIC   response = requests.request(method='POST', headers=headers, url=url, json=data_json)
# MAGIC   if response.status_code != 200:
# MAGIC     raise Exception(f'Request failed with status {response.status_code}, {response.text}')
# MAGIC   return response.json()

# COMMAND ----------

# MAGIC %python
# MAGIC new_data_pandas = new_data.toPandas()
# MAGIC print(new_data_pandas)

# COMMAND ----------

# MAGIC %python
# MAGIC # get some churn example

# COMMAND ----------

# MAGIC %python
# MAGIC # Model serving is designed for low-latency predictions on smaller batches of data
# MAGIC num_predictions = 5
# MAGIC served_predictions = score_model(new_data_pandas)
# MAGIC #model_evaluations = model.predict(new_data)
# MAGIC # Compare the results from the deployed model and the trained model
# MAGIC pd.DataFrame({
# MAGIC   "Served Model Prediction": served_predictions,
# MAGIC })
