# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

applicationid=dbutils.secrets.get(scope="adlskeyvault",key="clientIdd")

# COMMAND ----------

print(applicationid)

# COMMAND ----------

authenticationkey=dbutils.secrets.get(scope="adlskeyvault",key="clientsecretidd")

# COMMAND ----------

print(authenticationkey)

# COMMAND ----------

tenantId=dbutils.secrets.get(scope="adlskeyvault",key="tenant")

# COMMAND ----------

print(tenantId)

# COMMAND ----------

mountPoint="/mnt/input"
adlsAccountName="datastorageadls5253"
adlsContainerName="raw"
adlsFolderName="input"



# COMMAND ----------

endpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"

# COMMAND ----------

source= "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName


# COMMAND ----------

print(source)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationid,
           "fs.azure.account.oauth2.client.secret": authenticationkey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# COMMAND ----------

print(configs)

# COMMAND ----------

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source = source,mount_point = mountPoint,extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls "mnt/input"

# COMMAND ----------

filepath="dbfs:/mnt/input/mnm_dataset.csv"
df_mnm=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_mnm)

# COMMAND ----------

df_mnm.groupBy("Color").count().show()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze/calender/

# COMMAND ----------


