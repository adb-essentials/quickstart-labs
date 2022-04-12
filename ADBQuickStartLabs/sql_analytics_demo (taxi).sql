-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Attach this notebook to a high concurrency cluster WITH table ACLs enabled.
-- MAGIC ## Run the first two cells before the demo. (At least an hour)
-- MAGIC 
-- MAGIC The third cell grants access to the database for all users. You may want to make this more restrictive. 
-- MAGIC 
-- MAGIC This notebook includes cells meant to be typed or copied into the SQL query editor. It also contains markdown instructions for creating visualizations, and a script for presentation. 
-- MAGIC 
-- MAGIC For the demo: Use an x-large endpoint with photon enabled (if possible).

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dbacademy;
CREATE TABLE IF NOT EXISTS dbacademy.nyctaxi
USING delta
LOCATION "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/"


-- COMMAND ----------

DROP TABLE IF EXISTS dbacademy.nyctaxi_yr;
CREATE TABLE dbacademy.nyctaxi_yr PARTITIONED BY (p_year) AS
SELECT
  *,
  year(pickup_datetime) as p_year
FROM
  dbacademy.nyctaxi

-- COMMAND ----------

GRANT USAGE, CREATE, MODIFY, SELECT, READ_METADATA ON DATABASE dbacademy to `users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Speaker notes: 
-- MAGIC 
-- MAGIC Today we're going to create a dashboard to help us analyze taxi travel and revenue in a large city. This data is from a New York City open dataset. It's a little over 40 Gb of data. 
-- MAGIC 
-- MAGIC Let's start with a simple query. 
-- MAGIC 
-- MAGIC I'm going to open a New Query and type this into into the editor
-- MAGIC 
-- MAGIC When I open the query editor, I'm going to attach to my running endpoint and select the `dbacademy` database where my table is written. In the schema browser, I can see the table name and when I click on that, I can see the table schema information. This column name, `p_year`, let's me know this haas been partitioned by year, so now I know that it will be really fast and easy to pull data to just a single year at a time.
-- MAGIC 
-- MAGIC The first thing we want to look at is just the total amount of revenue generated in a single year. The most complete data in this set is from 2014, so we'll work with that one.
-- MAGIC   
-- MAGIC #### Start typing in the query below
-- MAGIC 
-- MAGIC When I type SQL key words and table or column names, the query editor offers autocomplete solutions. Also, I don't have to worry about formatting - you can use this button or CMD + Shift + F to automatically reformat a query. 
-- MAGIC 
-- MAGIC When I'm done, I'll hit the execute button and the results appear as a table below the editor. 
-- MAGIC 
-- MAGIC I'm going to name this query, "Total fares,"  and then save it. If I want others to be able to view and run this query, I can share it with a single user, a group, or all users in the workspace. 

-- COMMAND ----------

SELECT
  SUM(fare_amount) as total_fares,
  COUNT(fare_amount) as num_rides
FROM
  dbacademy.nyctaxi_yr
WHERE
  p_year = 2014

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Click on Add Visualization
-- MAGIC * Choose Visualization Type: Counter
-- MAGIC * Name your visualization in Visualization Name: Sum Fares
-- MAGIC * In Counter value column name, select total fares. 
-- MAGIC * Click on the format tab in the visualization editor. 
-- MAGIC * Add a dollar sign to "Formatting string Prefix"
-- MAGIC * Then, click save

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice that the table and the "Sum Fares" counter both appear as visualization for this query. I could also add an additional visualization if I wanted to, but I think we're done with this one. Before we leave this page, I'm just going to tag this query so that it's easy to find. Next to the query name, you have the option of adding a tag. I'll take this: taxi demo. Click ok. And now, let's go on to the next. 
-- MAGIC 
-- MAGIC * Click Queries
-- MAGIC * Click New Query
-- MAGIC 
-- MAGIC Now, let's take a look at the types of payments we've got coming in. In this query, I'm simply taking the sum of the total amount column grouped by payment type. 
-- MAGIC 
-- MAGIC I'll run this and then add a visualization. 

-- COMMAND ----------

SELECT
  payment_type,
  SUM(total_amount) as total_paid
FROM
  dbacademy.nyctaxi_yr
WHERE
  p_year = 2014
GROUP BY
  payment_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Click on Add Visualization
-- MAGIC * Name it Total Paid
-- MAGIC * Under Chart Type, select Pie
-- MAGIC * For x column, select payment type
-- MAGIC * For y column, select total_paid
-- MAGIC 
-- MAGIC We're just creating a simple pie chart here, but I want to point out that there are a lot of customization options available for all of these charts. You can easily change the colors and/or labels. 
-- MAGIC 
-- MAGIC For example, the column name appears by default at the top of this chart, but I can change that label to "Total Payments", for example. 
-- MAGIC 
-- MAGIC Note: Click on the Series tab of the Visualization editor. Change the Label to `Total Payments`
-- MAGIC 
-- MAGIC Now we can click Save and come back to the query. 
-- MAGIC 
-- MAGIC I'll name this query Payment Types and I'm also going to tag this one with "Taxi Demo." Click save and then go back out to my Queries menu.
-- MAGIC 
-- MAGIC When I filter by tag, I can see my two tagged queries.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Great. Now let's get an idea of where these trips are happening.  I have pickup latitudes and longitudes in my table. In a new query, I can use those to create a map of all the pickup locations. 
-- MAGIC 
-- MAGIC I'll name it and tag it. (Taxi Pickup Locations, taxi demo)

-- COMMAND ----------

SELECT
  pickup_longitude,
  pickup_latitude,
  COUNT(1) as rides,
  SUM(total_amount) as total_amount
FROM
  dbacademy.nyctaxi_yr
WHERE
  p_year = 2014
GROUP BY
  pickup_longitude,
  pickup_latitude

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Click on Add Visualization
-- MAGIC * Select Map (Markers)
-- MAGIC * For Latitude Column name, select pickup latitude
-- MAGIC * For Longitude column name, select pickup longitude
-- MAGIC * Group by rides
-- MAGIC * Click save 
-- MAGIC 
-- MAGIC Great - this gives us a good picture of where lots of activity is occurring. It looks like there are a couple of long rides here, but we can see that there pickups are all happening about where we would expect them to be. 
-- MAGIC 
-- MAGIC Now, let's do the same thing for dropoff locations, but in a new query. I'm going to Fork this one to make a copy, and then I'll have a good start. 
-- MAGIC 
-- MAGIC * Click the three dots in the upper right corner of the screen
-- MAGIC * Click Fork

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we have a new copy of that same query. The first thing I want to do is change the name to reflect that these are drop off locations. Notice that the tag carried over, so I can keep that. 
-- MAGIC 
-- MAGIC And now, I'll just change the column names to:
-- MAGIC 
-- MAGIC dropoff_longitude
-- MAGIC 
-- MAGIC dropoff_latitude
-- MAGIC 
-- MAGIC And I can execute that. 
-- MAGIC 
-- MAGIC If we look down in the Visualizations, we can see that the map also copied, but our revision to the query broke so now it's blank. 
-- MAGIC 
-- MAGIC Click on Edit Visualization. Change the latitude and longtitude column names to refer to the new  dropoff columns. 

-- COMMAND ----------

SELECT
  SUM(fare_amount) as total_fare,
  SUM(tip_amount) as total_tip,
  SUM(tolls_amount) as total_tolls,
  hour(pickup_datetime) as hour
FROM
  dbacademy.nyctaxi_yr
WHERE
  p_year = 2014
GROUP BY hour
ORDER BY hour

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Click on Add Visualization
-- MAGIC * Name the visualization Charge Types
-- MAGIC * Chart type: bar
-- MAGIC * Check: Horizontal Chart
-- MAGIC * y column: hour
-- MAGIC * x columns: total_fare, total_tip, total_tolls
-- MAGIC * Stacking: stack
-- MAGIC * Click save

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ok - let's make sure to name and tag this (Charges by Hour, taxi demo) and then I think we've got enough to build up our dashboard! 
-- MAGIC 
-- MAGIC We'll click on the Dashboards tab to get started. And then New Dashboard. 
-- MAGIC 
-- MAGIC This is the dashboard in edit mode. You can add a text box. This accepts basic Markdown and can be helpful for titles or to provide context. I'll write here that all of these charts will be about 
-- MAGIC 
-- MAGIC NYC Taxi Data, 2014
-- MAGIC 
-- MAGIC I can also add widgets, the visualizations we created. Let start by getting that total fares information up here. 
-- MAGIC 
-- MAGIC * Click on Total Fares
-- MAGIC * Choose the Counter visualization, Sum Fares. 
-- MAGIC * Click Add to Dashboard
-- MAGIC * You can drag, drop, and resize widgets as necessary. 
-- MAGIC 
-- MAGIC I'll go ahead and add the rest. 
-- MAGIC (Add the rest: Charges by hour, Payment Types, )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC One additional feature to note is that you can create parameterized queries so that the user can choose parameters to pass into queries. 
-- MAGIC 
-- MAGIC Let's say that we want to add an additional parameter to this "Charge by hour" chart. We can allow the user to focus in on a certain month, for example. 
-- MAGIC 
-- MAGIC From the dashboard, I can click on the the three vertical dots on the widget and jump back into the query. We can change it so that it includes the month of pickup in the GROUP BY and so that it is filtered by month. 
-- MAGIC 
-- MAGIC To create a parameter, you can type in these double curly braces OR use this button. The title is the name of the variable that will also be used in the query definition. You can either let the user type in a value or list of values, provide a drop-down list of choices, or have a list provided based on the results of another query. In this case, I know 12 months are represented here, so I'm just going to paste in this list of values. 
-- MAGIC 
-- MAGIC Then, when I execute, the results include only values from the selected month. 

-- COMMAND ----------

SELECT
  SUM(fare_amount) as total_fare,
  SUM(tip_amount) as total_tip,
  SUM(tolls_amount) as total_tolls,
  hour(pickup_datetime) as hour,
  month(pickup_datetime) as month
FROM
  dbacademy.nyctaxi_yr
WHERE
  p_year = 2014 AND month(pickup_datetime) = {{ month selection }} 
GROUP BY hour, month
ORDER BY hour


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Double click this cell and then copy and paste these values into the parameters dialog. 
-- MAGIC 
-- MAGIC 1
-- MAGIC 2
-- MAGIC 3
-- MAGIC 4
-- MAGIC 5
-- MAGIC 6
-- MAGIC 7
-- MAGIC 8
-- MAGIC 9
-- MAGIC 10
-- MAGIC 11
-- MAGIC 12
