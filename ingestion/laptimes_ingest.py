# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

laptimes_df=spark.read.schema(lap_times_schema).option('header',True).csv(f'{raw_path}/lap_times')

# COMMAND ----------

laptimes_df=laptimes_df.withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId', 'driver_id')\
                        .withColumn('ingestion_time', current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.laptimes"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/laptimes")
    delta_table.alias('existing').merge(
            laptimes_df.alias('incoming'),
            "existing.race_id = incoming.race_id AND existing.driver_id = incoming.driver_id AND existing.lap = incoming.lap")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    laptimes_df.write.mode('overwrite').option("path",f"{processed_path}/laptimes").format('delta').saveAsTable('processed.laptimes')
