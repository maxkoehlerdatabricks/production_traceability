# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC
# MAGIC In this example, we consider a simplified production process of a **gear shaft**.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Complete_Process.png?raw=true" width=100%>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Production steps**: The source of these data is MES as well as data that is directly recorded at the station.
# MAGIC
# MAGIC - **Equip_Raw**: In the first step the worker picks a raw part according to the work plan. The diameter of the cylindrical shank for the gear shaft is slightly larger than that
# MAGIC diameter of the finished part. The raw part is scanned and eqiped to the "Turning_Blank" station.
# MAGIC - **Turning_Blank**: The cylindrical shaft is rotated. First, the rough is done by roughing material abrasion until the desired outside diameter and length of the shank is reached. This is followed by a fine machining. After processing, a quality test is carried out, which measures the surface quality with the inline measuring system resulting in a decision of OK / NOK values per part. NOK parts are not further processed. Furthermore, the turing speed of the turning machine in round per minute (rpm) is recorded.
# MAGIC - **Mill**: The milling machine creates the characteristic surfaces for the transmission shaft. The thread is worked in and keyways are milled. This process gives the shaft its functionality and the interaction with other components is enabled.
# MAGIC - **Assembly**: In this simplified production process this step unifies all necessary assembly steps to produce the final product.
# MAGIC
# MAGIC **Logistics steps**: The source of these data is SAP.
# MAGIC
# MAGIC - **Package**: The final product is packaged and attached with a package id.
# MAGIC - **Shipment**: The package is shipped to the customer's warehouse.
# MAGIC - **Delivery**: The package id is delivered to the customer's production site.

# COMMAND ----------

# MAGIC %pip install networkx

# COMMAND ----------

import networkx as nx
import pyspark.sql.functions as f
from pyspark.sql.types import *
from graphframes import *

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
# MAGIC From a high level, each part that is produced and shipped will traverse the following production and logistics steps. This data set reflects the production process as hsown in the picture above.

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
# MAGIC ## The problem with use cases within and across multiple plants 

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily see that the data belongs to multiple plants

# COMMAND ----------

display(production_process_df.select("plant").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Use_Cases_Multiple_Plants_1.png?raw=true" width=49%>
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Use_Cases_Multiple_Plants_2.png?raw=true" width=49%>

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
# MAGIC The customer reports on two issues and recalls the respective package id's

# COMMAND ----------

error_type1_df = spark.read.table("error_type1_df")
display(error_type1_df)

# COMMAND ----------

error_type2_df = spark.read.table("error_type2_df")
display(error_type2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The supplier reports on quality issies in one of their batches and provides the respective 

# COMMAND ----------

supplier_batch_df = spark.read.table("supplier_batch_df")
display(supplier_batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the graph object

# COMMAND ----------

production_process_df = spark.read.table("production_process_df").drop("part_number")
display(production_process_df)

# COMMAND ----------

# Create edges_df
edge_df = (production_process_df.
              withColumn("src", f.concat_ws('/',f.col("BC_Child"),f.col("SID_Child"), f.col("plant"))).
              withColumn("dst", f.concat_ws('/',f.col("BC_Parent"),f.col("SID_Parent"), f.col("plant")))
)

# Create vertices_df and collect vertices metadata
vertices_df = (
  edge_df.select(f.col("src").alias("id"), f.col("BC_Child").alias("BC"), f.col("SID_Child").alias("SID"), f.col("plant")).
  union(
    edge_df.select(f.col("dst").alias("id"), f.col("BC_Parent").alias("BC"), f.col("SID_Parent").alias("SID"), f.col("plant"))
    ).
  distinct()
)

# COMMAND ----------

display(edge_df)

# COMMAND ----------

display(vertices_df)

# COMMAND ----------

# Save edges and vertices data
edge_df.write.mode("overwrite").saveAsTable("edge_df")
vertices_df.write.mode("overwrite").saveAsTable("vertices_df")

# COMMAND ----------

# MAGIC %md
# MAGIC The graph can eaisly be created fomr the edges and the vertices

# COMMAND ----------

g = GraphFrame(vertices_df, edge_df)

# COMMAND ----------


