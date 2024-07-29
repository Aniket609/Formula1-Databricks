# Databricks notebook source
dbutils.widgets.text('presentation path', '/mnt/aniketformula1dl/presentation')
presentation_path=dbutils.widgets.get('presentation path')

# COMMAND ----------

race_results_df=spark.read.format('delta').load(f"{presentation_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col
final_df=race_results_df.groupBy('race_year','driver_name','driver_nationality','team')\
                                .agg(sum('points').alias('total_points'),\
                                 count(when(col('position')==1,True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

driver_rank_window = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
driver_standing=final_df.withColumn('rank', rank().over(driver_rank_window))

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"presentation.driver_standing"):
    delta_table = DeltaTable.forPath(spark, f"{presentation_path}/driver_standing")
    delta_table.alias('existing').merge(
            driver_standing.alias('incoming'),
            "existing.race_year = incoming.race_year AND existing.driver_name = incoming.driver_name AND existing.rank = incoming.rank AND existing.team = incoming.team")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS presentation')
    driver_standing.write.mode('overwrite').option("path",f"{presentation_path}/driver_standing").format('delta').saveAsTable('presentation.driver_standing')
