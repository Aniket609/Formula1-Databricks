# Databricks notebook source
#this uses Service Principle based access to create mounts, with all secrets storeed in Azure Keyvault, accessing through secret scope
def createMount(storage_account_name, container_name):    
    application_id = dbutils.secrets.get(scope='formula1_scope', key="formula1-application-id")
    directory_id = dbutils.secrets.get(scope='formula1_scope', key="formula1-directory-id")
    client_secret = dbutils.secrets.get(scope='formula1_scope', key="formula1-client-secret")
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}
   
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        print(f"mount /mnt/{storage_account_name}/{container_name} already exists")
    else:
        dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
        print(f'mount /mnt/{storage_account_name}/{container_name} has been created')


# COMMAND ----------

#storage account and containers must be created before running this
createMount(storage_account_name="aniketformula1dl", container_name="raw")
createMount(storage_account_name="aniketformula1dl", container_name="processed")
createMount(storage_account_name="aniketformula1dl", container_name="presentation")

# COMMAND ----------

def vacuum_and_optimize_database(database):
    tables = spark.sql(f"SHOW TABLES IN {database} ").select("tableName").rdd.flatMap(lambda x: x).collect()
    for table in tables:
        spark.sql(f"VACUUM {database}.{table} RETAIN 0 HOURS")
        spark.sql(f"OPTIMIZE {database}.{table}")
    print(f"Database '{database}' has been vacuumed and optimized!")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
vacuum_and_optimize_database('processed')
vacuum_and_optimize_database('presentation')

