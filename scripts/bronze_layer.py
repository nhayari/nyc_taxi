# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists bronze_db;
# MAGIC show databases;
# MAGIC use bronze_db;
# MAGIC select current_database()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, substring_index
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType
import datetime

# COMMAND ----------

schema = StructType([
     StructField("VendorID", IntegerType(), True),
     StructField("tpep_pickup_datetime", TimestampType(), True),
     StructField("tpep_dropoff_datetime", TimestampType(), True),
     StructField("passenger_count", LongType(), True),
     StructField("trip_distance", DoubleType(), True),
     StructField("RatecodeID", LongType(), True),
     StructField("store_and_fwd_flag", StringType(), True),
     StructField("PULocationID", IntegerType(), True),
     StructField("DOLocationID", IntegerType(), True),
     StructField("payment_type", LongType(), True),
     StructField("fare_amount", DoubleType(), True),
     StructField("extra", DoubleType(), True),
     StructField("mta_tax", DoubleType(), True),
     StructField("tip_amount", DoubleType(), True),
     StructField("tolls_amount", DoubleType(), True),
     StructField("improvement_surcharge", DoubleType(), True),
     StructField("total_amount", DoubleType(), True),
     StructField("congestion_surcharge", DoubleType(), True),
     StructField("Airport_fee", DoubleType(), True)
 ])

aws_access_key = "**************************"
aws_secret_key = "**************************"
bucket_name= "nour-taxi-data"
file_path = f"s3a://{bucket_name}/*.parquet" 


# COMMAND ----------


df = (
    spark.read
    .schema(schema)
    .option("fs.s3a.access.key", aws_access_key)
    .option("fs.s3a.secret.key", aws_secret_key)
    .option("fs.s3a.endpoint", "s3.amazonaws.com")
    .parquet(file_path)
    # .load(f"s3a://{bucket_name}/*.parquet")
)

display(df)


# COMMAND ----------

# 5. Ajouter une colonne ingestion_timestamp + une colonne nom de fichier
df = df.withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("file_name", substring_index(col("_metadata.file_path"), "/", -1))

# -------------------------------
# 6. Calcul des nombres de lignes
# -------------------------------

# Nombre de lignes dans la dataframe pyspark à ingérer
new_rows_count = df.count()

# Vérifier si la table existe dans le Catalogue Spark et compter le nombre de lignes existantes dans cette table
if "raw_trips" in [t.name for t in spark.catalog.listTables("bronze_db")]:
    existing_rows_count = spark.table("bronze_db.raw_trips").count()
else:
    existing_rows_count = 0

# Affichage des informations
print(f"Nombre de lignes à ingérer : {new_rows_count}")
print(f"Nombre de lignes existantes dans la table : {existing_rows_count}")

# 7. Sauvegarde des données dans une table Delta
df.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("raw_trips")

# 8. Contrôle de cohérence au niveau des nombres de lignes
final_rows_count = spark.table("bronze_db.raw_trips").count()
expected_rows = existing_rows_count + new_rows_count

if final_rows_count != expected_rows:
    raise Exception(f"Contrôle de cohérence échoué : Le nombre de lignes dans la table finale {final_rows_count} ne correspond pas au nombre de lignes attendues {expected_rows}") 
else:
    print(f"Contrôle de cohérence réussi : Le nombre de lignes dans la table finale {final_rows_count} correspond au nombre de lignes attendues {expected_rows}")

# # 9. Archiver les fichiers Parquet traités
# archive_folder = source_folder + "archive/run_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/"

# for f in files:
#     dbutils.fs.mv(f.path, archive_folder + f.name)

# print(f"{len(files)} fichiers archivés dans {archive_folder}")

# # Afficher le contenu du dossier archive
# display(dbutils.fs.ls(archive_folder))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM raw_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_name FROM raw_trips ORDER BY file_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED raw_trips;

# COMMAND ----------

# zone_file_path = "Workspace/Users/myemail@domain.com/anyc_taxi_data/bronze_data/taxi_zone_lookup.csv"
# df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_file_path)
# display(df_zones)