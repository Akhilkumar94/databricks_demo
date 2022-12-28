# Databricks notebook source
# MAGIC %fs ls /mnt/input

# COMMAND ----------

df_calender=spark.read.format("csv").option("header",True).option("infer_schema",True).load("dbfs:/mnt/input/calendar.csv")

# COMMAND ----------

display(df_calender)

# COMMAND ----------

df_listing=spark.read.format("csv").option("header",True).option("infer_schema",True).load("dbfs:/mnt/input/listings.csv")


# COMMAND ----------



df_listin_details=spark.read.format("csv").option("header",True).option("infer_schema",True).load("dbfs:/mnt/input/listings_details.csv")

# COMMAND ----------




df_neighborhood=spark.read.format("csv").option("header",True).option("infer_schema",True).load("dbfs:/mnt/input/neighbourhoods.csv")


# COMMAND ----------





df_review=spark.read.format("csv").option("header",True).option("infer_schema",True).load("dbfs:/mnt/input/reviews.csv")



# COMMAND ----------


df_review_details=spark.read.format("csv").option("header",True).option("infer_schema",True).load("dbfs:/mnt/input/reviews_details.csv")

# COMMAND ----------


df_geojson=spark.read.format("json").option("multipline",True).load("dbfs:/mnt/input/neighbourhoods.geojson")

# COMMAND ----------

display(df_geojson)

# COMMAND ----------

df_calender.write.mode("overwrite").format("parquet").save("/mnt/bronze/calender/")

# COMMAND ----------

df_listing.write.mode("overwrite").format("parquet").save("/mnt/bronze/listing/")

# COMMAND ----------

df_listin_details.write.mode("overwrite").format("parquet").save("/mnt/bronze/listing_details/")

# COMMAND ----------

df_neighborhood.write.mode("overwrite").format("parquet").save("/mnt/bronze/neighborhood/")

# COMMAND ----------

df_review.write.mode("overwrite").format("parquet").save("/mnt/bronze/review/")

# COMMAND ----------

df_review_details.write.mode("overwrite").format("parquet").save("/mnt/bronze/review_details/")

# COMMAND ----------


