# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Problem Solving with Traceability

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Goal**: Trace back from a bunch of erroneous package id’s to test data in production
# MAGIC
# MAGIC **Situation**:
# MAGIC - Products were produced by the manufacturer and then shipped to the customer
# MAGIC - The customer observed that a significant amount of products is out of the specifications and recalls a complete time range
# MAGIC - The manufacturer wants to 
# MAGIC     - Explain the issue with the production data, 
# MAGIC     - Predict the occurrence of the issue to reduce the number of products of the complete time range to a couple of products, i.e. identify the broken parts on barcode level.
# MAGIC
# MAGIC **Motivation**: Significantly reduce the non conformance costs of the manufacturer
# MAGIC

# COMMAND ----------


