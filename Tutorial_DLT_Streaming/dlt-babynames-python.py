# Databricks notebook source
import dlt
from pyspark.sql.functions import *
## This comming from pevius notebook ca
## /Volumes/mycompanyacme/src_sources_acme/dtl_sources
#my_catalog = "mycompanyacme"
#my_schema = "src_sources_acme"
#my_volume = "dtl_sources"
##
my_catalog = "mycompanyacme"
my_schema = "src_sources_acme"
my_volume = "dtl_sources"

volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# COMMAND ----------

@dlt.table(
    comment="Popular baby first names in New York. This data was ingested from the New York State Department of Health."
)
def baby_names_raw():
    # df = spark.read.csv(volume_path + filename, header=True, inferSchema=True)
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("inferSchema", True)
        .option("header", True)
        .load(volume_path)
    )
    df_renamed_column = df.withColumnRenamed("First Name", "First_Name")
    return df_renamed_column

# COMMAND ----------

@dlt.table(
    comment="New York popular baby first name data cleaned and prepared for analysis."
)
@dlt.expect("valid_first_name", "First_Name IS NOT NULL")
@dlt.expect_or_fail("valid_count", "Count > 0")
def baby_names_prepared():
    return (
        spark.read.table("LIVE.baby_names_raw")
            .withColumnRenamed("Year", "Year_Of_Birth")
            .select("Year_Of_Birth", "First_Name", "Count")
    )

# COMMAND ----------

@dlt.table(
    comment="A table summarizing counts of the top baby names for New York for 2021."
)
def top_baby_names_2021():
    return (
        spark.read.table("LIVE.baby_names_prepared")
            .filter(expr("Year_Of_Birth == 2021"))
            .groupBy("First_Name")
            .agg(sum("Count").alias("Total_Count"))
            .sort(desc("Total_Count"))
            .limit(10)
    )
