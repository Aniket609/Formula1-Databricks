# Databricks notebook source
dbutils.widgets.text("raw_path", "")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])


# COMMAND ----------

races_df = spark.read.option('header',True).schema(races_schema).csv(f"{raw_path}/races.csv")

# COMMAND ----------

races_df=races_df.withColumnRenamed("raceId", "race_id")\
                .withColumnRenamed('year','race_year')\
                .withColumnRenamed('circuitId','circuit_id')\
                .withColumnRenamed('name','race_name')\
                .withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                .withColumn('ingestion_time', current_timestamp())\
                .drop('date','time','url')

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"processed.races"):
    delta_table = DeltaTable.forPath(spark, f"{processed_path}/races")
    delta_table.alias('existing').merge(
            races_df.alias('incoming'),
            "existing.race_id = incoming.race_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS processed')
    races_df.write.mode('overwrite').option("path",f"{processed_path}/races").format('delta').saveAsTable('processed.races')
