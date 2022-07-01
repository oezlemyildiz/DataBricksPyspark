# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

df_demo = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display (df_demo)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

df_demo.select(count("*")).show()
df_demo.select(count("race_name")).show()
df_demo.select(countDistinct("race_name")).show()



# COMMAND ----------

df_demo.select(sum("points")).show()
df_demo.filter("race_name='Daniel Ricciardo'").select(sum("points").alias("sum_points"),countDistinct("race_name").alias("cnt_race_name")).show()


# COMMAND ----------

df_demo.groupBy("driver_name")\
.sum("points")\
.show()

# COMMAND ----------

"""
COUNT DISTINCT bu şekilde ÇALIŞMIYOR, dataframe olduğu için
df_demo.groupBy("driver_name")\
.sum("points")\
.countDistinct("race_name")
.show()
"""

df_demo.groupBy("driver_name")\
.agg(sum("points"),countDistinct("race_name"))\
.show(5)

# COMMAND ----------

df_demo2=df_demo.groupBy("race_year","driver_name")\
.agg(sum("points").alias("total_points"),countDistinct("race_name").alias("cnt_race_name"))
display(df_demo2.head(3))


# COMMAND ----------

from pyspark.sql.window import Window as window
from pyspark.sql.functions import desc,rank

driverRankSpech = window.partitionBy("race_year").orderBy(desc("total_points"))

df_demo2.withColumn("rank",rank().over(driverRankSpech)).show(5)

# COMMAND ----------

