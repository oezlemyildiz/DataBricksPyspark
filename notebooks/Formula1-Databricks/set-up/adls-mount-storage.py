# Databricks notebook source
"""
storage_account_name = "formula1dlv4"
client_id            = "deb04b35-8c46-43b2-a148-3cf33db29953"
tenant_id            = "16854914-8a0e-4cbd-9550-be79b95138d2"
client_secret        = "yNe8Q~G8uZm9cxiwMIPzFp8dtKKIsFyxQhk.Rbus"
"""

# COMMAND ----------

storage_account_name = "formula1dlv4"
client_id            = dbutils.secrets.get(scope="formula1-scope",key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="formula1-scope",key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="formula1-scope",key="databricks-app-client-secret")

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dlv4/raw")
dbutils.fs.unmount("/mnt/formula1dlv4/processed")

# COMMAND ----------

"""
container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)
"""

# COMMAND ----------

def mnt_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mnt_adls("raw")

# COMMAND ----------

mnt_adls("processed")



# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlv4/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlv4/processed")

# COMMAND ----------

#dbutils.secrets.help()


# COMMAND ----------

#dbutils.secrets.list("formula1-scope")

# COMMAND ----------

#dbutils.secrets.get(scope="formula1-scope",key="databricks-app-client-id")

# COMMAND ----------

#commit