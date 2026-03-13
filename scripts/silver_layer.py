# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver_db;
# MAGIC show databases;
# MAGIC USE silver_db;
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Préparer la table Silver (si elle n'existe pas encore)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS processed_trips 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM bronze_db.raw_trips
# MAGIC WHERE 
# MAGIC   passenger_count > 0
# MAGIC   AND trip_distance > 0
# MAGIC   AND total_amount > 0
# MAGIC   AND tpep_pickup_datetime < tpep_dropoff_datetime 
# MAGIC   AND tip_amount >= 0
# MAGIC   AND payment_type = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM processed_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_name FROM processed_trips ORDER BY file_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Charger seulement les nouvelles données de la table Bronze
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH new_data AS (
# MAGIC   SELECT 
# MAGIC     * 
# MAGIC   FROM bronze_db.raw_trips b  
# MAGIC   WHERE b.ingestion_timestamp > (
# MAGIC     SELECT MAX(ingestion_timestamp) 
# MAGIC     FROM processed_trips
# MAGIC   )
# MAGIC   AND b.passenger_count > 0
# MAGIC   AND b.trip_distance > 0
# MAGIC   AND b.total_amount > 0
# MAGIC   AND b.tpep_pickup_datetime < tpep_dropoff_datetime 
# MAGIC   AND b.tip_amount >= 0
# MAGIC   AND b.payment_type = 1
# MAGIC )
# MAGIC
# MAGIC MERGE INTO processed_trips s
# MAGIC USING new_data n
# MAGIC ON s.file_name = n.file_name 
# MAGIC AND s.tpep_pickup_datetime = n.tpep_pickup_datetime
# MAGIC AND s.tpep_dropoff_datetime = n.tpep_dropoff_datetime
# MAGIC AND s.ingestion_timestamp = n.ingestion_timestamp
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_name FROM processed_trips ORDER BY file_name;