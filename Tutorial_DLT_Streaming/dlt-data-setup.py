# Databricks notebook source
# MAGIC %md
# MAGIC Original notebook  Tutorial from https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/tutorial-pipelines
# MAGIC

# COMMAND ----------

my_catalog = "<catalog-name>"
my_schema = "<schema-name>"
my_volume = "<volume-name>"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
filename = "babynames.csv"

dbutils.fs.cp(download_url, volume_path + filename)

# COMMAND ----------

# MAGIC %md
# MAGIC this notebook is comming from:
# MAGIC https://docs.databricks.com/en/_extras/notebooks/source/dlt-data-setup.html
# MAGIC

# COMMAND ----------

## /Volumes/mycompanyacme/src_sources_acme/dtl_sources
my_catalog = "mycompanyacme"
my_schema = "src_sources_acme"
my_volume = "dtl_sources"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
filename = "babynames.csv"

dbutils.fs.cp(download_url, volume_path + filename)
