# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# TODO
username = 'Yuan'
#file_path = [file.path for file in dbutils.fs.ls("/FileStore/movies/") if "movie_" in file.path]

# COMMAND ----------

peopleDimPath = f"/dbacademy/{username}/movies/people/"
classicPipelinePath = f"/dbacademy/{username}/movies/"

landingPath = classicPipelinePath + "landing/"
rawPath = classicPipelinePath + "raw/"
bronzePath = classicPipelinePath + "bronze/"
silverPath = classicPipelinePath + "silver/"
silverQuarantinePath = classicPipelinePath + "silverQuarantine/"
goldPath = classicPipelinePath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS movies_{username}")
spark.sql(f"USE movies_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities
