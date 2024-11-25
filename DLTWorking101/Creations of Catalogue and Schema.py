# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create a catalog named "mycompanyacme"
# MAGIC CREATE CATALOG IF NOT EXISTS mycompanyacme;
# MAGIC
# MAGIC -- Switch to the new catalog
# MAGIC USE CATALOG mycompanyacme;
# MAGIC
# MAGIC -- Create a schema within the catalog named "datacustomer"
# MAGIC CREATE SCHEMA IF NOT EXISTS mycompanyacme.datacustomer;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant usage and create permissions to a specific user or group (replace "username" with the Databricks username)
# MAGIC GRANT USAGE ON CATALOG mycompanyacme TO `myuser`;
# MAGIC GRANT CREATE ON SCHEMA mycompanyacme.datacustomer TO `myuser`;
# MAGIC
