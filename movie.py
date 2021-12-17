# Databricks notebook source
spark.sql(f"CREATE DATABASE IF NOT EXISTS movieshop")
spark.sql(f"USE movieshop")


# COMMAND ----------

from pyspark.sql.functions import *
file_path = [file.path for file in dbutils.fs.ls("/FileStore/movies/") if "movie_" in file.path]
print(file_path)

# COMMAND ----------

raw_df = (spark.read
         .option("multiline", "true")
          .option("inferSchema", "true")
         .format("json")
         .load(file_path)).select(explode("movie").alias("movies"))
raw_df = (raw_df.select("movies.*"))
display(raw_df)

# COMMAND ----------

display(raw_df)
