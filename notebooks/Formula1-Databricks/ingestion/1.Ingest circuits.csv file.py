# Databricks notebook source
# MAGIC %md
# MAGIC Read CSV File 

# COMMAND ----------

#display(dbutils.fs.mounts())
#%fs
#ls /mnt/formula1dlv4/raw

# COMMAND ----------

"""
df_circuits = spark.read\
.options(header=True, inferschema=True)\
.csv('/mnt/formula1dlv4/raw/circuits.csv')
"""

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType

# COMMAND ----------

circuits_schema = StructType(fields=
     [StructField("circuitId", IntegerType(),False),
     StructField("circuitRef", StringType(),True),
     StructField("name", StringType(),True),
     StructField("location", StringType(),True),
     StructField("country", StringType(),True),
     StructField("lat", StringType(),True),
     StructField("lng", StringType(),True),
     StructField("alt", StringType(),True),
     StructField("url", StringType(),True)]
     #StructField("ingestionDate", DateType(),False)
)

# COMMAND ----------

df_circuits = spark.read\
.options(header=True, schema=circuits_schema)\
.csv('/mnt/formula1dlv4/raw/circuits.csv')

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

#df_circuits.describe().show()
df_circuits.printSchema()

# COMMAND ----------

#commit

# COMMAND ----------

