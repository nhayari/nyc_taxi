# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION `ext-storage-taxi`;
# MAGIC

# COMMAND ----------

# Dans un notebook Databricks
dbutils.fs.ls("s3://nour-taxi-data/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC SELECT * FROM silver_db.processed_trips LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold_db;
# MAGIC USE gold_db;
# MAGIC SELECT current_database();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.taxi_zones LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE processed_trips_with_zones 
# MAGIC USING DELTA 
# MAGIC AS
# MAGIC SELECT
# MAGIC   s.*,
# MAGIC   pu_zone.Borough AS pickup_borough,
# MAGIC   do_zone.Borough AS dropoff_borough,
# MAGIC   pu_zone.Zone AS pickup_zone,
# MAGIC   do_zone.Zone AS dropoff_zone,
# MAGIC   DATE(s.tpep_pickup_datetime) AS pickup_date,
# MAGIC   HOUR(s.tpep_pickup_datetime) AS pickup_hour,
# MAGIC   HOUR(s.tpep_dropoff_datetime) AS dropoff_hour,
# MAGIC   DAYOFWEEK(s.tpep_pickup_datetime) AS pickup_day_of_week
# MAGIC FROM silver_db.processed_trips s
# MAGIC LEFT JOIN bronze_db.taxi_zones pu_zone 
# MAGIC   ON s.PULocationID = pu_zone.LocationID
# MAGIC LEFT JOIN bronze_db.taxi_zones do_zone 
# MAGIC   ON s.DOLocationID = do_zone.LocationID
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM processed_trips_with_zones;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM processed_trips_with_zones LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_name FROM processed_trips_with_zones ORDER BY file_name;