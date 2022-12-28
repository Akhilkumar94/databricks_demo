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

mountPoint="/mnt/silver"
adlsAccountName="datastorageadls5253"
adlsContainerName="silver"
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

filepath="dbfs:/mnt/input/listings.csv"
df_mnm=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_mnm)

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists listings_1;
# MAGIC create temporary table listings_1
# MAGIC using csv
# MAGIC options (
# MAGIC  path "dbfs:/mnt/input/listings.csv", header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Counting the total no of records

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from listings_1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listing the first 100 records

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from listings_1 limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## calculating the distinct neighbourhood 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(neighbourhood)) as neighbourhood_citys
# MAGIC from listings_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select  `host_name`,`host_id`,`neighbourhood`,`room_type`,
# MAGIC         count (*) as number_of_reviews
# MAGIC from    listings_1
# MAGIC group by
# MAGIC         `host_name`,`host_id`,`neighbourhood`,`room_type`
# MAGIC order by
# MAGIC        number_of_reviews desc
# MAGIC limit 50;

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists neighbourhoods_1;
# MAGIC create temporary table neighbourhoods_1
# MAGIC using csv
# MAGIC options (
# MAGIC  path "dbfs:/mnt/input/neighbourhoods.csv", header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from neighbourhoods_1

# COMMAND ----------

df_nhood = spark.table('neighbourhoods_1')

# COMMAND ----------

df_nhood.write.format("parquet").save("/mnt/silver/neighbourhoods")

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists ReviewDetails_1;
# MAGIC create temporary table ReviewDetails_1
# MAGIC using csv
# MAGIC options (
# MAGIC  path "dbfs:/mnt/input/reviews_details.csv", header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from ReviewDetails_1

# COMMAND ----------

df_review = spark.table('ReviewDetails_1')

# COMMAND ----------

df_review.write.format("parquet").save("/mnt/silver/reviews")

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists listingsDetails_1;
# MAGIC create temporary table listingsDetails_1
# MAGIC using csv
# MAGIC options (
# MAGIC  path "dbfs:/mnt/input/listings_details.csv", header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from listingsDetails_1

# COMMAND ----------

# MAGIC %md
# MAGIC ##creating dataframes from the tables

# COMMAND ----------

df_listing = spark.table('listings_1')
df_listingDetails = spark.table('listingsDetails_1')
df_nhood = spark.table('neighbourhoods_1')
df_review = spark.table('ReviewDetails_1')

# COMMAND ----------

# MAGIC %md
# MAGIC ###What property types are available in airbnb listing and distribution of those?

# COMMAND ----------

display(df_listingDetails.groupBy('room_type').count())

# COMMAND ----------

from pyspark.sql.functions import col
df_listingDetails.groupBy('property_type').count().orderBy(col('count').desc()).show()

# COMMAND ----------

df_listingDetails.write.format("parquet").save("/mnt/silver/listingdetails_1")

# COMMAND ----------

df_listingDetails.groupBy('property_type',"price").agg(({'price':'average'})).show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct price from listingsDetails_1

# COMMAND ----------


import pyspark.sql.functions as f
from pyspark.sql.window import Window

df_neigh_count = df_listingDetails.groupBy('neighbourhood').agg(({'neighbourhood':'count'})).withColumnRenamed("count(neighbourhood)","count")

display(df_neigh_count)

# COMMAND ----------

df_neigh_count.write.format("parquet").save("/mnt/silver/Neighboor_count")

# COMMAND ----------


