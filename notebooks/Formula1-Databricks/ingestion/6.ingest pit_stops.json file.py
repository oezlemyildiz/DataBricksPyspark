# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType
pit_stops_schema=StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("stop",StringType(),True),
    StructField("lab",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("duration",StringType(),True),
    StructField("milseconds",IntegerType(),True)
    ])

# COMMAND ----------

df_pit_stops= spark.read\
.schema(pit_stops_schema)\
.option("multiLine",True)\
.json("/mnt/formula1dlv4/raw/pit_stops.json")

# COMMAND ----------

display(df_pit_stops)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat
df_pit_stops_renamed= df_pit_stops.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driverId")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df_lap_times_renamed.write.mode("overwrite").parquet("/mnt/formula1dlv4/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlv4/processed/pit_stops"))

# COMMAND ----------

