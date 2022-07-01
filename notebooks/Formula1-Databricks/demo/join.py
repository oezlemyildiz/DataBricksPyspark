# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

df_races=spark.read.parquet(f"{processed_folder_path}/races")\
.filter("race_year ==2019 and race_round <=5")

# COMMAND ----------

df_circuits=spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

df_races_circuits_join = df_circuits.join(df_races, df_circuits.circuit_id==df_races.race_circuit_id, "inner")\
.select(df_circuits.circuit_name, df_circuits.circuit_location,df_circuits.circuit_country,df_races.race_name,df_races.race_round)

# COMMAND ----------

display(df_races_circuits_join)

# COMMAND ----------

df_circuits_races_left = df_circuits.join(df_races, df_circuits.circuit_id==df_races.race_circuit_id, "left")\
.select(df_circuits.circuit_name, df_circuits.circuit_location,df_circuits.circuit_country,df_races.race_name,df_races.race_round)

# COMMAND ----------

df_circuits_races_right = df_circuits.join(df_races, df_circuits.circuit_id==df_races.race_circuit_id, "right")\
.select(df_circuits.circuit_name, df_circuits.circuit_location,df_circuits.circuit_country,df_races.race_name,df_races.race_round)

# COMMAND ----------

df_circuits.count()

# COMMAND ----------

df_races.count()

# COMMAND ----------

df_circuits_races_full = df_circuits.join(df_races, df_circuits.circuit_id==df_races.race_circuit_id, "full")\
.select(df_circuits.circuit_id,df_races.race_circuit_id, df_circuits.circuit_name, df_circuits.circuit_location,df_circuits.circuit_country,df_races.race_name,df_races.race_round)

# COMMAND ----------

display(df_circuits_races_full)

# COMMAND ----------

#just take left table column but inner work logic
df_races_circuits_semi = df_circuits.join(df_races, df_circuits.circuit_id==df_races.race_circuit_id, "semi")
display(df_races_circuits_semi)

# COMMAND ----------

#it is work opsito semi join
#everthing in left table,  but not in right table, sağ tabloda olup solda olmayan verileri veriyor
df_races_circuits_anti = df_circuits.join(df_races, df_circuits.circuit_id==df_races.race_circuit_id, "anti")
display(df_races_circuits_anti)

# COMMAND ----------

display(df_races.filter("race_circuit_id=2"))

# COMMAND ----------

display(df_circuits.crossJoin(df_races))

# COMMAND ----------

