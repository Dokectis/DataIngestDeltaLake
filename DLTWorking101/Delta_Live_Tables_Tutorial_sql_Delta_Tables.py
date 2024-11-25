# Databricks notebook source
import dlt
from pyspark.sql.functions import *



# COMMAND ----------

# MAGIC %md
# MAGIC Load the Raw Data: Define two tables, one for each CSV dataset. These tables will be the "raw" layer in the DLT pipeline.

# COMMAND ----------

@dlt.table
def raw_orders_DLT():
    return spark.sql("""
    SELECT * FROM mycompanyacme.src_sources_acme.orders
    """)

@dlt.table
def raw_customers_DLT():
    return spark.sql("""
    SELECT * FROM mycompanyacme.src_sources_acme.customers
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Intermediate Tables: Create transformed tables by joining the raw data. Here, we will aggregate the orders by customer and calculate the total amount per customer.

# COMMAND ----------

@dlt.table
def orders_with_customer_DLT():
    return dlt.read("raw_orders_DLT") \
        .join(dlt.read("raw_customers_DLT"), "customer_id") \
        .select("order_id", "customer_id", "customer_name", "customer_city", "amount")

@dlt.table
def customer_order_totals_DLT():
    return dlt.read("orders_with_customer_DLT") \
        .groupBy("customer_id", "customer_name") \
        .agg(sum("amount").alias("total_amount"))

# COMMAND ----------

# MAGIC %md
# MAGIC Define the Final Table: The final table in this example provides the summarized view of customer orders.

# COMMAND ----------

@dlt.table
def final_customer_summary_DLT():
    return dlt.read("customer_order_totals_DLT")

