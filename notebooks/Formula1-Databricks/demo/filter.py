# Databricks notebook source
df_races=spark.read.parquet("/mnt/formula1dlv4/processed/races")

# COMMAND ----------

#sql
#df_races_filtered= df_races.filter("race_year=2019 and round <=5")
#df_races_filtered= df_races.where("race_year=2019 and race_round <=5")
#python 
#df_races_filtered= df_races.filter((df_races["race_year"]==2019) & (df_races["race_round"] <=5))
df_races_filtered= df_races.where((df_races["race_year"]==2019) & (df_races["race_round"] <=5))

# COMMAND ----------

display(df_races_filtered)

# COMMAND ----------

