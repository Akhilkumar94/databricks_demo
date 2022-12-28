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

mountPoint="/mnt/gold"
adlsAccountName="datastorageadls5253"
adlsContainerName="gold"
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
df_listings=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_listings)

# COMMAND ----------

filepath="dbfs:/mnt/input/calendar.csv"
df_calendar=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_calendar)

# COMMAND ----------

filepath="dbfs:/mnt/input/neighbourhoods.csv"
df_neighbourhoods=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_neighbourhoods)

# COMMAND ----------

filepath="dbfs:/mnt/input/reviews.csv"
df_reviews=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_reviews)

# COMMAND ----------

filepath="dbfs:/mnt/input/reviews_details.csv"
df_reviews_details=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df_reviews_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count the listing files

# COMMAND ----------

df_count=df_reviews.groupBy('listing_id').count().show()

# COMMAND ----------

df_reviews.write.format("parquet").save("/mnt/gold/count_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### listing more reviews

# COMMAND ----------

from pyspark.sql.functions import *
df_listings.alias('list_of_files').join(df_reviews.alias('review_files'),                 col('list_of_files.listing_id')==col('review_files.listing_id')).select('list_of_files.listing_id').groupBy('list_of_files.listing_id').count().sort(desc('count')).show()

# COMMAND ----------

df_listings.write.format("parquet").save("/mnt/gold/listing_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### listings that doesnt have the reviews

# COMMAND ----------

df_listings.alias('listing_data').join(df_reviews.alias('review_data'), col('listing_data.listing_id')==col('review_data.listing_id'),how='left').select('listing_data.listing_id','review_data.listing_id').filter(col('review_data.listing_id').isNull()).distinct().count()
df_listings.write.format("parquet").save("/mnt/gold/no_review")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atleast 30 reviews done by reviewers

# COMMAND ----------

df_reviews.groupBy('listing_id').count().filter(col('count') > 30).sort(desc('count')).show()
df_reviews.write.format("parquet").save("/mnt/gold/reviews>30")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter the reviews that have review-ID

# COMMAND ----------

df_review_fil = df_reviews_details.filter(col('reviewer_id').cast('int').isNotNull())
display(df_review_fil)
df_review_fil.write.format("parquet").save("/mnt/gold/proper_review")

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary table reviews
# MAGIC using csv
# MAGIC options (
# MAGIC  path "dbfs:/mnt/input/reviews_details.csv", header "true"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments based on reviews

# COMMAND ----------

# MAGIC 
# MAGIC %sql 
# MAGIC 
# MAGIC select 
# MAGIC regexp_extract(comments,'(good|excellent|amazing)',1) as word, comments
# MAGIC from reviews
# MAGIC where regexp_extract(comments,'(good|excellent|amazing)',1)!=''

# COMMAND ----------

df_commentreview = spark.table('reviews')

# COMMAND ----------

df_commentreview.write.format("parquet").save("/mnt/gold/comments")
