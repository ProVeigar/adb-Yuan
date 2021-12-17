# Databricks notebook source

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from urllib.request import urlretrieve
from pyspark.sql.functions import from_unixtime, dayofmonth, month, hour
from delta import DeltaTable
from datetime import datetime
import time
MOVIE_DELTA = "movies.delta"


# COMMAND ----------

def prepare_activity_data(landingPath) -> bool:
    file_path = [file.path for file in dbutils.fs.ls("/FileStore/movies/") if "movie_" in file.path]

    movieIngest = (
        spark.read.option("multiline","true")
      .option("inferSchema", "true")
        .format("json")
        .load(file_path)
        #.withColumn("time", from_unixtime("time"))
        .select(
            explode("movie").alias("movies"),
        )
        .write.format("delta")
        .save(landingPath + MOVIE_DELTA)
    )


def ingest_classic_data(hours: int = 1) -> bool:
    next_batch = spark.read.format("delta").load(landingPath + MOVIE_DELTA)
    next_batch = (next_batch.select("movies.*"))
    file_name = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    (next_batch.write.format("json").save(rawPath + file_name))

    # move file out of directory and rename
    new_json_file = [
        file.path for file in dbutils.fs.ls(rawPath + file_name) if "part" in file.path
    ][0]
    dbutils.fs.mv(new_json_file, rawPath + file_name + ".txt")
    dbutils.fs.rm(rawPath + file_name, recurse=True)

    #classicIngest = DeltaTable.forPath(spark, landingPath + MOVIE_DELTA)

    '''delete_match = """
        ingest.name = next.name AND
        ingest.time = next.time
    """

    (
        classicIngest.alias("ingest")
        .merge(next_batch.alias("next"), delete_match)
        .whenMatchedDelete()
        .execute()
    )
'''
    return True


def untilStreamIsReady(namedStream: str, progressions: int = 3) -> bool:
    queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    print("The stream {} is active and ready.".format(namedStream))
    return True

