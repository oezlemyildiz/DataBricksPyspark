# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType, FloatType
results_schema = StructType(fields=[
    StructField("constructorId", IntegerType(),True),
    StructField("driverId", IntegerType(),False),
    StructField("fastestLap", IntegerType(),False),
    StructField("fastestLapSpeed", FloatType(),False),
    StructField("fastestLapTime", StringType(),False),
    StructField("grid", IntegerType(),False),
    StructField("laps", IntegerType(),False),
    StructField("milliseconds", StringType(),False),
    StructField("number", IntegerType(),False),
    StructField("points", FloatType(),False),
    StructField("position", IntegerType(),False),
    StructField("positionOrder", IntegerType(),False),
    StructField("positionText", StringType(),False),
    StructField("raceId", IntegerType(),False),
    StructField("rank", StringType(),False),
    StructField("resultId", IntegerType(),False),
    StructField("statusId", StringType(),False),
    StructField("time", StringType(),False)])


# COMMAND ----------

df_result = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

df_result_drop= df_result.drop(col("statusId"))

# COMMAND ----------

display(df_result_drop)

# COMMAND ----------

df_result_final = df_result_drop\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("fastestLap", "fastest_lap")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("resultId", "result_id")\
    .withColumn("ingest_date", current_timestamp())


# COMMAND ----------

df_result_final.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlv4/processed/results.json

# COMMAND ----------

dbutils.notebook.exit("Success")