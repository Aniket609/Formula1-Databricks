# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiline',True).json(f'{raw_path}/qualifying')

# COMMAND ----------

qualifying_df=qualifying_df.withColumnRenamed("qualifyId", "id")\
                            .withColumnRenamed("raceId", "race_id")\
                            .withColumnRenamed("driverId", "driver_id")\
                            .withColumnRenamed("constructorId", "constructor_id")\
                            .withColumn('ingestion_time', current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.qualifying"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/qualifying")
    delta_table.alias('existing').merge(
            qualifying_df.alias('incoming'),
            "existing.id = incoming.id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    qualifying_df.write.mode('overwrite').option("path",f"{processed_path}/qualifying").format('delta').saveAsTable('processed.qualifying')
