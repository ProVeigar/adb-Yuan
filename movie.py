# Databricks notebook source
spark.sql(f"CREATE DATABASE IF NOT EXISTS movieshop")
spark.sql(f"USE movieshop")


# COMMAND ----------

from pyspark.sql.functions import *
file_path = [file.path for file in dbutils.fs.ls("/FileStore/movies/") if "movie_" in file.path]
print(file_path)

# COMMAND ----------

raw_df1 = (spark.read
         .option("multiline", "true")
          .option("inferSchema", "true")
         .format("json")
         .load(file_path)).select(explode("movie").alias("movies"))
raw_df1 = (raw_df1.select("movies.*"))
raw_df1.count()


# COMMAND ----------

raw_df.drop(*)
raw_df.count()

# COMMAND ----------

display(raw_df1)
