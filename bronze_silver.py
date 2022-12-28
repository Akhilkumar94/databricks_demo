# Databricks notebook source
# MAGIC %fs ls /mnt/bronze/listing

# COMMAND ----------

df_listing=spark.read.format("parquet").load("dbfs:/mnt/bronze/listing")


# COMMAND ----------

display(df_listing)

# COMMAND ----------


df_calender=spark.read.format("parquet").load("dbfs:/mnt/bronze/calender")


# COMMAND ----------

display(df_calender)

# COMMAND ----------

df_calender_listing=df_calender.alias("a").join(df_listing.alias("b"),df_calender.listing_id==df_listing.id,"inner").select("b.*","a.listing_id")

# COMMAND ----------

display(df_calender_listing)

# COMMAND ----------

df_Private=df_calender_listing.filter(df_calender_listing['room_type']=="Private room")

# COMMAND ----------

df_Private.count()

# COMMAND ----------

df_calender_listing.select("neighbourhood").distinct().count()

# COMMAND ----------

from pyspark.sql.functions import count,sum
df_calender_listing.groupby("host_id","room_type").agg(count("room_type"),sum("price").alias("sum_cust")).show()

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count
display(df_listing.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_listing.columns]
   ))

# COMMAND ----------

df_Private.write.mode("overwrite").format("parquet").partitionBy("neighbourhood").save("/mnt/silver/neighbourhood")

# COMMAND ----------


