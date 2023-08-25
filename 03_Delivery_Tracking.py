# Databricks notebook source
# MAGIC %run ./_resources/00_setup $reset_all_data=false

# COMMAND ----------

#Get a supplier batch and trace forwards to the package id's
# you might have to delete the last process steps of the graph, probaply take plant = 2 batch = 2 or 3, as other items have already been used
# use aggregated messaging
supplier_batch_df = spark.read.table("supplier_batch_df")
display(supplier_batch_df)

# COMMAND ----------


