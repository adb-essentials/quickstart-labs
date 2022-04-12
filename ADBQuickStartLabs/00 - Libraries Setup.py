# Databricks notebook source
import shutil
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.mllib.stat import Statistics
from pyspark.ml.stat import ChiSquareTest
from pyspark.sql import functions
from pyspark.sql.functions import isnan, when, count, col
import pandas as pd
import numpy as np
import matplotlib.pyplot as mplt
import matplotlib.ticker as mtick

# COMMAND ----------

#import the necessary libraries
import os
import mlflow
from pyspark.ml.regression import GeneralizedLinearRegression,RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, VectorIndexer,StandardScaler,IndexToString
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table

# COMMAND ----------

Databricks_Token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
Workspace = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").getOrElse(None)

# COMMAND ----------

import requests
response = requests.get(
  'https://%s/api/2.0/preview/sql/data_sources' % (WorkspaceURL),
  headers={'Authorization': 'Bearer %s' % DATABRICKS_TOKEN}
)

# COMMAND ----------

datasource = response.json()[0]["id"]

# COMMAND ----------

query1 = {
  "data_source_id": datasource,
  "query": """SELECT COUNT(1) FROM kkbox.transactions;

-- SELECT * FROM kkbox.transactions;

--SELECT COUNT(1) FROM kkbox.members;

--SELECT * FROM kkbox.members;""",
  "name": "Browse Tables",
  "description": "Browse KKBOX tables.",
}

# COMMAND ----------

import requests
response = requests.post(
  'https://%s/api/2.0/preview/sql/queries' % (WorkspaceURL),
  headers={'Authorization': 'Bearer %s' % DATABRICKS_TOKEN},
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
  'https://%s/api/2.0/preview/sql/queries' % (WorkspaceURL),
  headers={'Authorization': 'Bearer %s' % DATABRICKS_TOKEN},
  json=query2
)

# COMMAND ----------

response.json()
