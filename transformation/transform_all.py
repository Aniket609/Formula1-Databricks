# Databricks notebook source
dbutils.widgets.text('processed_path','/mnt/aniketformula1dl/processed')
processed_path= dbutils.widgets.get('processed_path')
dbutils.widgets.text('presentation_path','/mnt/aniketformula1dl/presentation')
presentation_path= dbutils.widgets.get('presentation_path')

# COMMAND ----------

dbutils.notebook.run("../transformation/race_results",0,{'processed_path':processed_path,'presentation_path':presentation_path})

# COMMAND ----------

dbutils.notebook.run("../transformation/driver_standings",0,{'presentation_path':presentation_path})

# COMMAND ----------

dbutils.notebook.run("../transformation/constructor_standings",0,{'presentation_path':presentation_path})
