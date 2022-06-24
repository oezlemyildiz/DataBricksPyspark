# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")


# COMMAND ----------

#Hive DDL style schema
constructors_schema = "constructorId INTEGER,constructorRef STRING,name STRING,nationality STRING,url STRING"
df_constructors = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col
#df_constructors_drop= df_constructors.drop("url")
#df_constructors_drop= df_constructors.drop(df_constructors["url"])
df_constructors_drop= df_constructors.drop(col("url"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_constructors_final= df_constructors_drop.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("constructorRef","constructor_ref")\
.withColumnRenamed("name","constructor_name")\
.withColumnRenamed("nationality","constructor_nationality")\
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(df_constructors_final)

# COMMAND ----------

df_constructors_final.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlv4/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")