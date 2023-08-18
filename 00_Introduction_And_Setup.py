# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC blablabla bla

# COMMAND ----------

# MAGIC %pip install networkx

# COMMAND ----------

import networkx as nx

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ./_resources/00_setup $reset_all_data=true

# COMMAND ----------

# MAGIC %md
# MAGIC # Explaining the Data

# COMMAND ----------

print(f"We use the table in the schama '{dbName}' in the catalog '{catalogName}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The process graph draft
# MAGIC From a high level, each part that is produced and shipped will traverse the following production and logistics steps

# COMMAND ----------

sample_process_graph_df = spark.read.table("process_graph_draft").drop("Start_Time", "End_Time", "Link_to_Data")
display(sample_process_graph_df)

# COMMAND ----------

G = nx.from_pandas_edgelist(sample_process_graph_df.toPandas(), source='SID_Parent', target='SID_Child', edge_attr='Assembly_Type', create_using=nx.DiGraph())
nx.draw_spring(G, with_labels= True, font_size = 7, font_color = "red", node_color = "lightgrey")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The process graph on barcode level
# MAGIC As parts traverse the production and logistics barcodes and station id's are tracked. We expand this table by appending respective rows for the logistics steps where the BC's are the respective package id's and the stations are chosen to be explanatory for the respective process. Start times and end times for each process step are given as well as the assembly type. 

# COMMAND ----------

production_process_df = spark.read.table("production_process_df").drop("part_number")
display(production_process_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily see that the data belongs to multiple plants

# COMMAND ----------

display(production_process_df.select("plant").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test, supplier and customer data along the production value chain

# COMMAND ----------

# MAGIC %md
# MAGIC Measurement data of the turning process is recoreded in rounds per minute (rpm). The data is an "endless" timeseries.

# COMMAND ----------

measurement_series_turning_rpm = spark.read.table("measurement_series_turning_rpm").drop("part_number")
display(measurement_series_turning_rpm)

# COMMAND ----------

# MAGIC %md
# MAGIC The customer reports om two issues and recalls the respective package id's

# COMMAND ----------

error_type1_df = spark.read.table("error_type1_df")
display(error_type1_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC The supplier reports on quality issies in one of their batches and provides the respective 

# COMMAND ----------


