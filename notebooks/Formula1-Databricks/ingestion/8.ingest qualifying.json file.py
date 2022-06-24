# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType
qualify_schema=StructType(fields=[
  StructField("qualifyId",IntegerType(),False),
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("constructorId",IntegerType(),True),
     StructField("number", IntegerType(), True),
    StructField("position",IntegerType(),True),
    StructField("q1",StringType(),True),
    StructField("q2",StringType(),True),
    StructField("q3",StringType(),True)
    ])


# COMMAND ----------

df_qualify= spark.read\
.schema(qualify_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(df_qualify)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat
df_qualify_renamed= df_qualify.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driverId")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df_qualify_renamed.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlv4/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")