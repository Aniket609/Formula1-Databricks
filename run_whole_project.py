# Databricks notebook source
dbutils.widgets.text('raw_path','/mnt/aniketformula1dl/raw')
raw_path= dbutils.widgets.get('raw_path')
dbutils.widgets.text('processed_path','/mnt/aniketformula1dl/processed')
processed_path= dbutils.widgets.get('processed_path')
dbutils.widgets.text('presentation_path','/mnt/aniketformula1dl/presentation')
presentation_path= dbutils.widgets.get('presentation_path')

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/aniketchatterjee2000ee@outlook.com/ingestion/ingest_all",0, {"raw_path":raw_path, "processed_path":processed_path})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/aniketchatterjee2000ee@outlook.com/transformation/transform_all",0, {"processed_path":processed_path, "presentation_path":presentation_path})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/aniketchatterjee2000ee@outlook.com/presentation/present_all",0)

# COMMAND ----------

#uncomment this and run to delete table history and optimize your dataase

#def vacuum_and_optimize_database(database):
    #tables = spark.sql(f"SHOW TABLES IN {database} ").select("tableName").rdd.flatMap(lambda x: x).collect()
    #for table in tables:
        #spark.sql(f"VACUUM {database}.{table} RETAIN 0 HOURS")
        #spark.sql(f"OPTIMIZE {database}.{table}")
    #print(f'Database {database} has been vacuumed and optimized!')
#spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
#vacuum_and_optimize_database('processed')
#vacuum_and_optimize_database('presentation')
