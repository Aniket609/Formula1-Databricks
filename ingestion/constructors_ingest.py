# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).option('header',True).json(f"{raw_path}/constructors.json")

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed("constructorId", "constructor_id")\
                                .withColumnRenamed("constructorRef", "constructor_ref")\
                                .withColumnRenamed("name", "constructor_name")\
                                .withColumn('ingestion_timestamp', current_timestamp())\
                                .drop('url')

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.constructors"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/constructors")
    delta_table.alias('existing').merge(
            constructors_df.alias('incoming'),
            "existing.constructor_id = incoming.constructor_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    constructors_df.write.mode('overwrite').option("path",f"{processed_path}/constructors").format('delta').saveAsTable('processed.constructors')
