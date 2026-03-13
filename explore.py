# Databricks notebook source
# MAGIC %md
# MAGIC ## étape 0- Lister le contenu

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))



# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/airlines"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## étape 1 — Lecture fichiers

# COMMAND ----------

# Lire tous les fichiers CSV du dossier airlines
df = spark.read.csv("/databricks-datasets/airlines/part-00000", header=True, inferSchema=True, nullValue="NA")

# Afficher le schéma (types des colonnes)
df.printSchema()

# COMMAND ----------


df.write.format("delta").mode("overwrite").saveAsTable("airlines_local2")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Retard par origin

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Origin, AVG(ArrDelay) AS retard_moyen, COUNT(*) AS nb_vols
# MAGIC FROM airlines_temp
# MAGIC GROUP BY Origin
# MAGIC ORDER BY retard_moyen DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UniqueCarrier, AVG(ArrDelay) AS retard_moyen, COUNT(*) AS nb_vols
# MAGIC FROM airlines_temp
# MAGIC GROUP BY UniqueCarrier
# MAGIC ORDER BY retard_moyen DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## retard par mois

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Month, AVG(ArrDelay) AS retard_moyen
# MAGIC FROM airlines_temp
# MAGIC GROUP BY Month
# MAGIC ORDER BY Month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retard moyen par trajet Origin -> Dest

# COMMAND ----------

# Retard moyen par trajet Origin -> D
import pyspark.sql.functions as F 
trajet = (
    df.groupBy("Origin", "Dest")
    .agg(F.avg("ArrDelay").alias("retard_moyen"), F.count("*").alias("nb_vols"))
    .orderBy(F.desc("retard_moyen"))
)

trajet.show(10)


# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("airlines_local")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM airlines_local;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## étape 2 — Nettoyage et préparation

# COMMAND ----------

# Sélectionner seulement les colonnes utiles
 = df_s.select("Year", "Month", "DayofMonth", "Origin", "Dest", "ArrDelay")

# Supprimer les lignes avec valeurs nulles sur les colonnes importantes
df_small = df_s.dropna(subset=["Origin", "Dest", "ArrDelay"])

# Vérifier le résultat
df_small.count()    


# COMMAND ----------

# MAGIC %md
# MAGIC ## étape 3 — Création d’une vue temporaire SQL

# COMMAND ----------


# Exemple de requête SQL
result = spark.sql("""
  SELECT Origin, Dest,COUNT(*) AS nb_vols, round(AVG(ArrDelay), 2) AS retard_moyen
  FROM airlines_temp
  GROUP BY Origin,Dest
  ORDER BY retard_moyen DESC
  LIMIT 10
""")

result.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## étape 4 — Manipulation PySpark native (sans SQL)

# COMMAND ----------

from pyspark.sql import functions as F

# Calculer le nombre de vols et le retard moyen par aéroport de départ
stats = (
    df_small.groupBy("Origin")
    .agg(
        F.count("*").alias("nb_vols"),
        F.round(F.avg("ArrDelay"),2).alias("retard_moyen")
    )
    .orderBy(F.desc("retard_moyen"))
)

stats.show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## étape 5 — Sauvegarde temporaire en table interne

# COMMAND ----------

df_small.write.format("delta").mode("overwrite").saveAsTable("airlines_local")

spark.sql("SELECT COUNT(*) FROM airlines_local").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## étape - 6 — Analyse temporelle

# COMMAND ----------

monthly = (
    df_small.groupBy("Year", "Month")
    .agg(F.avg("ArrDelay").alias("retard_moyen"))
    .orderBy("Year", "Month")
)

monthly.show(12)


# COMMAND ----------

# MAGIC %md
# MAGIC ## étape 7 — Bonus (petit défi)

# COMMAND ----------

(df_small.filter(df_small.Year == 1987)
 .groupBy("Origin", "Dest")
 .agg(F.avg("ArrDelay").alias("retard_moyen"))
 .orderBy("retard_moyen")
 .show(15))
