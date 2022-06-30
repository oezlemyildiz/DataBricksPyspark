# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

df_drivers = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 


# COMMAND ----------

df_constructors = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("constructor_name", "team")


# COMMAND ----------

df_circuits = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

df_races = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

df_results = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

df_race_circuits= df_races.join(df_circuits, df_races.race_circuit_id==df_circuits.circuit_id, "inner")\
.select (df_races.race_id, df_races.race_year, df_races.race_name, df_races.race_date, df_circuits.circuit_location)

# COMMAND ----------

df_race_results = df_results.join(df_race_circuits, df_results.race_id == df_race_circuits.race_id)\
.join(df_drivers, df_results.driver_id== df_drivers.driver_id)\
.join(df_constructors, df_results.constructor_id ==df_constructors.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_final = df_race_results.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality","team", "grid", "fastest_lap", "race_time", "points", "position") \
                         

# COMMAND ----------

display(df_final.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(df_final.points.desc()))

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

