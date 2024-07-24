# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

schema= StructType(fields=[StructField('raceId', IntegerType(), False),
                           StructField('driverId', IntegerType(),False),
                           StructField("stop", StringType(), True),
                            StructField("lap", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("duration", StringType(), True),
                            StructField("milliseconds", IntegerType(), True)
                             ])

# COMMAND ----------

pitstops_df=spark.read.schema(schema).option('multiline',True).json(f'{raw_path}/pit_stops.json')

# COMMAND ----------

pitstops_df=pitstops_df.withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId', 'driver_id')\
                        .withColumn('ingestion_time', current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.pitstops"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/pitstops")
    delta_table.alias('existing').merge(
            pitstops_df.alias('incoming'),
            "existing.race_id = incoming.race_id AND existing.driver_id = incoming.driver_id AND existing.stop = incoming.stop")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    pitstops_df.write.mode('overwrite').option("path",f"{processed_path}/pitstops").format('delta').saveAsTable('processed.pitstops')
