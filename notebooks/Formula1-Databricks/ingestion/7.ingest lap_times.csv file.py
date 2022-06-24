# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType
lap_times_schema=StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("lab",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("milseconds",IntegerType(),True)
    ])

# COMMAND ----------

#.csv("/mnt/formula1dlv4/raw/lap_times/lap_times_split*.csv")
df_lap_times= spark.read\
.schema(lap_times_schema)\
.csv("/mnt/formula1dlv4/raw/lap_times/")

# COMMAND ----------

display(df_lap_times)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat
df_lap_times_renamed= df_lap_times.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_Id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df_lap_times_renamed.write.mode("overwrite").parquet("/mnt/formula1dlv4/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlv4/processed/lap_times"))

# COMMAND ----------

