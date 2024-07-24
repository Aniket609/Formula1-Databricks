# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lit, concat, current_timestamp

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json(f"{raw_path}/drivers.json")

# COMMAND ----------

drivers_df=drivers_df.withColumnRenamed("driverId", "driver_id")\
                    .withColumnRenamed("driverRef", "driver_ref")\
                    .withColumn('name', concat(col("name.forename"), lit(" "), col("name.surname")))\
                    .withColumn('ingestion_date', current_timestamp())\
                    .drop('url')

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.drivers"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/drivers")
    delta_table.alias('existing').merge(
            drivers_df.alias('incoming'),
            "existing.driver_id = incoming.driver_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    drivers_df.write.mode('overwrite').option("path",f"{processed_path}/drivers").format('delta').saveAsTable('processed.drivers')
