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
         .load(file_path)).select(explode("movie").alias("movies"),"movie")
#raw_df1 = (raw_df1.select("movies.id","movies.genres"))
raw_df1.count()


# COMMAND ----------


raw_df.count()

# COMMAND ----------

display(raw_df1)

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

#silver_genres = spark.read.table("genres_bronze").filter("status = 'new' ")
silver_genres = raw_df1.select(explode("movie.genres").alias("genres"),"movies")
silver_genres = silver_genres.select("genres.id","genres.name","movies")

# COMMAND ----------

display(silver_genres)

# COMMAND ----------


