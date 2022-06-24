# Databricks notebook source
# MAGIC %md
# MAGIC Ingest read Race file

# COMMAND ----------

#df_race_file= spark.read.options(header=True,inferschema=True).csv("/mnt/formula1dlv4/raw/races.csv")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType
races_schema = StructType(fields=
     [StructField("raceId", IntegerType(),False),
     StructField("year", IntegerType(),True),
     StructField("round", IntegerType(),True),
     StructField("circuitId", IntegerType(),True),
     StructField("name", StringType(),True),
     StructField("date", DateType(),True),
     StructField("time", StringType(),True),
     StructField("url", StringType(),True)])


# COMMAND ----------

df_races = spark.read\
.option("header",True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/races.csv")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat,to_timestamp
df_races_withcolumn=df_races\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("year", "race_year")\
.withColumnRenamed("round", "race_round")\
.withColumnRenamed("circuitId", "race_circuit_id")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("date", "race_date")\
.withColumnRenamed("time", "race_time")\
.withColumn("env",lit("eargast"))\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("race_timestamp",to_timestamp(concat(col("race_date"),lit(' '), col("race_time")),"yyyy-MM-dd HH:mm:ss"))



# COMMAND ----------

df_races_final= df_races_withcolumn.drop(col("url"))


# COMMAND ----------

df_races_final.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlv4/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")