# Databricks notebook source
# MAGIC %md
# MAGIC Read CSV File 

# COMMAND ----------

#display(dbutils.fs.mounts())
#%fs
#ls /mnt/formula1dlv4/raw

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType

# COMMAND ----------

circuits_schema = StructType(fields=
     [StructField("circuitId", IntegerType(),False),
     StructField("circuitRef", StringType(),True),
     StructField("name", StringType(),True),
     StructField("location", StringType(),True),
     StructField("country", StringType(),True),
     StructField("lat", DoubleType(),True),
     StructField("lng", DoubleType(),True),
     StructField("alt", StringType(),True),
     StructField("url", StringType(),True)])

# COMMAND ----------

"""
df_circuits = spark.read\
.options(header=True, inferschema=True)\
.csv('/mnt/formula1dlv4/raw/circuits.csv')
"""

#.csv('/mnt/formula1dlv4/raw/circuits.csv')
df_circuits = spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/circuits.csv")


# COMMAND ----------

#df_circuits.describe().show()
df_circuits.printSchema()

# COMMAND ----------

"""
display(df_circuits.select("circuitId","circuitRef"))
display(df_circuits.select(df_circuits.circuitId,df_circuits.circuitRef))
display(df_circuits.select(df_circuits["circuitId"],df_circuits["circuitRef"]))
"""
from pyspark.sql.functions import col
df_circuits_selected= df_circuits.select(col("circuitId").alias("circuit_Id"),
                           col("circuitRef").alias("circuit_Ref"),
                           col("name").alias("circuit_name"),
                           col("location").alias("circuit_location"),
                           col("country").alias("circuit_country"),
                           col("lat").alias("circuit_lat"),
                           col("lng").alias("circuit_lng")
                          )


# COMMAND ----------

#.withColumn("ingestion_date",current_timestamp())\
from pyspark.sql.functions import current_timestamp, lit
df_circuits_renammed= df_circuits_selected\
.withColumnRenamed("circuit_lat", "circuit_latitute")\
.withColumnRenamed("circuit_lng", "circuit_lengitute")\
.withColumn("env",lit("eargast"))

# COMMAND ----------

df_circuits_final= f_add_ingestion_date(df_circuits_renammed)

# COMMAND ----------

#df_circuits_renammed.write.mode("overwrite").parquet("dbfs:/mnt/formula1dlv4/processed/circuits")
#df_circuits_renammed.write.mode("overwrite").parquet("/mnt/formula1dlv4/processed/circuits")
df_circuits_final.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")


# COMMAND ----------

#df_circuits_parguet= spark.read.parquet("dbfs:/mnt/formula1dlv4/processed/circuits")
#display(df_circuits_parguet)
#%fs
#ls /mnt/formula1dlv4/processed/circuits