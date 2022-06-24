# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result=dbutils.notebook.run("1.Ingest circuits.csv file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("2.ingest race.csv file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("3.ingest constructors.json file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("4.ingest driver.json file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("5.Ingest result.json file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("6.ingest pit_stops.json file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("7.ingest lap_times.csv file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("8.ingest qualifying.json file",0,{"p_data_source":"Ergast API"})
print(v_result)

# COMMAND ----------

