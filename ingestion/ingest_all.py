# Databricks notebook source
dbutils.widgets.text("raw_path", "/mnt/aniketformula1dl/raw")
raw_path = dbutils.widgets.get("raw_path")
dbutils.widgets.text("processed_path", "/mnt/aniketformula1dl/processed")
processed_path = dbutils.widgets.get("processed_path")

# COMMAND ----------

dbutils.notebook.run("../ingestion/circuits_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/constructors_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/drivers_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/laptimes_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/pitstops_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/qualifying_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/races_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})

# COMMAND ----------

dbutils.notebook.run("../ingestion/results_ingest",0,{'raw_path': raw_path, 'processed_path': processed_path})
