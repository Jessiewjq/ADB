# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - ETL into a Silver table
# MAGIC 
# MAGIC We need to perform some transformations on the data to move it from bronze to silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest raw data using composable functions
# MAGIC 1. Use composable functions to write to the Bronze table
# MAGIC 1. Develop the Bronze to Silver Step
# MAGIC    - Extract and transform the raw string to columns
# MAGIC    - Quarantine the bad data
# MAGIC    - Load clean data into the Silver table
# MAGIC 1. Update the status of records in the Bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Bronze Paths

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Delta Architecture
# MAGIC Next, we demonstrate everything we have built up to this point in our
# MAGIC Delta Architecture.
# MAGIC 
# MAGIC We do so not with the ad hoc queries as written before, but now with
# MAGIC composable functions included in the file `classic/includes/main/python/operations`.
# MAGIC You should check this file for the correct arguments to use in the next
# MAGIC three steps.
# MAGIC 
# MAGIC 🤔 You can refer to `plus/02_bronze_to_silver` if you are stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the `rawDF` DataFrame
# MAGIC 
# MAGIC **Exercise:** Use the function `read_batch_raw` to ingest the newly arrived
# MAGIC data.

# COMMAND ----------

movies_silver = movie_bronze_to_silver(movies_bronze).distinct()
display(movies_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Raw Data
# MAGIC 
# MAGIC **Exercise:** Use the function `transform_raw` to ingest the newly arrived
# MAGIC data.

# COMMAND ----------

# ANSWER
transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion
# MAGIC 
# MAGIC The DataFrame `transformedRawDF` should now have the following schema:
# MAGIC 
# MAGIC ```
# MAGIC datasource: string
# MAGIC ingesttime: timestamp
# MAGIC status: string
# MAGIC value: string
# MAGIC p_ingestdate: date
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("datasource", StringType(), False),
        StructField("ingesttime", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("value", StringType(), True),
        StructField("p_ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")

# COMMAND ----------

silver_movie = transform_bronze_movie(bronzemovieDF)

# COMMAND ----------

display(silver_movie)

# COMMAND ----------

silver_movie.count()

# COMMAND ----------

silver_movie_clean = silver_movie.filter((silver_movie.RunTime >= 0) & (silver_movie.Budget >= 1000000))
silver_movie_quarantine = silver_movie.filter((silver_movie.RunTime < 0) | (silver_movie.Budget < 1000000))

# COMMAND ----------

 silver_movie_clean.select('Movie_ID',
                            'Title',
                            'Budget',
                            'OriginalLanguage',
                            'RunTime',
                            'genres',
                            'value'
  )  
  .write.format("delta")
  .mode('append')
  .save(silverPathMovie)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{silverPathMovie}"
"""
)

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTableMovie = DeltaTable.forPath(spark, bronzePathMovie)
silverAugmentedMovie = (
    silver_movie_clean
    .withColumn("status", lit("loaded"))
)

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
  bronzeTableMovie.alias("bronze")
  .merge(silverAugmentedMovie.alias("clean"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
)


# COMMAND ----------

silverAugmentedMovie = (
  silver_movie_quarantine
  .withColumn("status", lit("quarantined"))
)

update_match = "bronze.value = quarantine.value"
update = {"status": "quarantine.status"}

(
  bronzeTableMovie.alias("bronze")
  .merge(silverAugmentedMovie.alias("quarantine"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
)