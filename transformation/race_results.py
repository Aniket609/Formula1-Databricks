# Databricks notebook source
dbutils.widgets.text('processed path', '/mnt/aniketformula1dl/processed')
processed_path = dbutils.widgets.get('processed path')
dbutils.widgets.text('presentation path', '/mnt/aniketformula1dl/presentation')
presentation_path=dbutils.widgets.get('presentation path')

# COMMAND ----------

drivers_df=spark.read.format('delta').load(f'{processed_path}/drivers')
circuits_df=spark.read.format('delta').load(f'{processed_path}/circuits')
constructors_df=spark.read.format('delta').load(f'{processed_path}/constructors')
races_df=spark.read.format('delta').load(f'{processed_path}/races')
results_df=spark.read.format('delta').load(f'{processed_path}/results')

# COMMAND ----------

joined_df1=circuits_df.join(other=races_df,on=circuits_df['circuit_id']==races_df['circuit_id'],how='inner')
joined_df1=joined_df1.select(joined_df1['race_id'], joined_df1['race_year'],joined_df1['race_name'],joined_df1['race_timestamp'].alias('race_date'),joined_df1['location'].alias('circuit_location'))

# COMMAND ----------

joined_df2=joined_df1.join(other=results_df, on=joined_df1['race_id']==results_df['race_id'], how='inner')
joined_df2=joined_df2.select(joined_df2['race_year'],joined_df2['race_name'],joined_df2['race_date'],joined_df2['circuit_location'],joined_df2['grid'],joined_df2['fastest_lap'], joined_df2['time'].alias('race_time'),joined_df2['points'],joined_df2['driver_id'],joined_df2['constructor_id'], joined_df2['position'])

# COMMAND ----------

joined_df3=joined_df2.join(other=drivers_df, on=joined_df2['driver_id']==drivers_df['driver_id'], how='inner')\
                    

joined_df3=joined_df3.select(joined_df3['race_year'],\
                            joined_df3['race_name'],\
                            joined_df3['race_date'],\
                            joined_df3['circuit_location'],\
                            joined_df3['name'].alias('driver_name'),\
                            joined_df3['number'].alias('driver_number'),\
                            joined_df3['nationality'].alias('driver_nationality'),\
                            joined_df3['grid'],\
                            joined_df3['fastest_lap'],\
                            joined_df3['race_time'],\
                            joined_df3['points'],\
                            joined_df3['constructor_id'],\
                            joined_df3['position'])

# COMMAND ----------

joined_df4= joined_df3.join(other=constructors_df, on=joined_df3['constructor_id']==constructors_df['constructor_id'], how='inner')

race_results_df=joined_df4.select(joined_df4['race_year'],\
                            joined_df4['race_name'],\
                            joined_df4['race_date'],\
                            joined_df4['circuit_location'],\
                            joined_df4['driver_name'],\
                            joined_df4['driver_number'],\
                            joined_df4['driver_nationality'],\
                            joined_df4['constructor_name'].alias('team'),\
                            joined_df4['grid'],\
                            joined_df4['fastest_lap'],\
                            joined_df4['race_time'],\
                            joined_df4['points'],\
                            joined_df4['position'])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
race_results_df= race_results_df.withColumn('created_date', current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists(f"presentation.race_results"):
    delta_table = DeltaTable.forPath(spark, f"{presentation_path}/race_results")
    delta_table.alias('existing').merge(
            race_results_df.alias('incoming'),
            "existing.race_year = incoming.race_year AND existing.race_date = incoming.race_date AND existing.team = incoming.team AND existing.driver_name = incoming.driver_name")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    spark.sql('CREATE DATABASE IF NOT EXISTS presentation')
    race_results_df.write.mode('overwrite').option("path",f"{presentation_path}/race_results").format('delta').saveAsTable('presentation.race_results')

# COMMAND ----------

race_results_calculated=race_results_df.select('race_year','team', 'driver_name','position','points')

# COMMAND ----------

from pyspark.sql.functions import col
race_results_calculated=race_results_calculated.filter(col('position')<=10).withColumn('calculated_points', 11-col('position'))

# COMMAND ----------


spark.sql('CREATE DATABASE IF NOT EXISTS presentation')
race_results_calculated.write.mode('overwrite').option("path",f"{presentation_path}/race_results_calculated").format('delta').saveAsTable('presentation.race_results_calculated')
