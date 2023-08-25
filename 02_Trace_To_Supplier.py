# Databricks notebook source
# MAGIC %run ./_resources/00_setup $reset_all_data=false

# COMMAND ----------

#Get the error type 2 data from the customer and trace back to supplier batches
# Use networkx maybe show in and out degrees
# apply in pandas
# egograph

# COMMAND ----------

error_type2_df = spark.read.table("error_type2_df")
display(error_type2_df)

# COMMAND ----------

supplier_batch_df = spark.read.table("supplier_batch_df")
display(supplier_batch_df)

# COMMAND ----------


