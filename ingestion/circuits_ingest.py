# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_date

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])


# COMMAND ----------

circuits_df=spark.read.option('header',True).schema(circuits_schema).csv(f"{raw_path}/circuits.csv")

# COMMAND ----------

circuits_df=circuits_df.withColumnRenamed('circuitId', 'circuit_id')\
        .withColumnRenamed('circuitRef', 'circuit_ref')\
        .withColumnRenamed('name', 'circuit_name')\
        .withColumnRenamed('lat', 'latitude')\
        .withColumnRenamed('lng', 'longitude')\
        .withColumnRenamed('alt', 'altitude')\
        .withColumn('ingestion_date', current_date())

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.circuits"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/circuits")
    delta_table.alias('existing').merge(
            circuits_df.alias('incoming'),
            "existing.circuit_id = incoming.circuit_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    circuits_df.write.mode('overwrite').option("path",f"{processed_path}/circuits").format('delta').saveAsTable('processed.circuits')
