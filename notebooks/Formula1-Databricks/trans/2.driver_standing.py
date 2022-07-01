# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

df_race_result = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

df_driver_standing = df_race_result\
.groupBy("race_year", "driver_name","driver_nationality","team")\
.agg(sum("points").alias("total_points"),\
    count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(df_driver_standing.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.window import Window as window
from pyspark.sql.functions import desc, rank,asc


driver_rank_spech = window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
df_final = df_driver_standing.withColumn("rank",rank().over(driver_rank_spech))
display(df_final)

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standing")

# COMMAND ----------

