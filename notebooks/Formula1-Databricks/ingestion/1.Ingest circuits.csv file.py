# Databricks notebook source
# MAGIC %md
# MAGIC Read CSV File 

# COMMAND ----------

#display(dbutils.fs.mounts())
#%fs
#ls /mnt/formula1dlv4/raw

# COMMAND ----------



# COMMAND ----------

df_circuits = spark.read.options(header="true").csv('/mnt/formula1dlv4/raw/circuits.csv')

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

