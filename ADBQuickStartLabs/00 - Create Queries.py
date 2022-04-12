# Databricks notebook source
Databricks_Token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
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
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
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
  'https://%s/api/2.0/preview/sql/queries' % (Workspace),
  headers={'Authorization': 'Bearer %s' % Databricks_Token},
  json=query2
)

# COMMAND ----------

response.json()
