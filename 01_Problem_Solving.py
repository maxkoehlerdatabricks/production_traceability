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
# MAGIC **Prerequisites**: Run the notebook *00_Introduction_And_Setup*
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The input to this analysis is a set barcodes that the customer identified to be broken. These parts were classified as OK in our inline measuremnt system.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup $reset_all_data=false

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *
from graphframes import *

# COMMAND ----------

error_type1_df = spark.read.table("error_type1_df")
display(error_type1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - We would like to analyze the data measured at the station "Turning_Blank". However
# MAGIC   - From the package id we do not know which plant the product comes from
# MAGIC   - We do not know the barcodes of the product at the station "Turning_Blank"
# MAGIC   - The measuremnt data is just an "endless" time seires and we do not know at what part of the time seires to consider for which barcode
# MAGIC - If our analyses reveils a deficiency in production that completely explains the issue, we can
# MAGIC   - Gain trust bei explaining the issue to the customer
# MAGIC   - Identify the effected barcodes
# MAGIC   - Reduce the number of barcodes that are effected by the recall from the complete time range to a small list

# COMMAND ----------

production_process_df = spark.read.table("production_process_df").drop("part_number")
display(production_process_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Complete_Process.png?raw=true" width=100%>

# COMMAND ----------

# MAGIC %md
# MAGIC We will use the production process graph to trace backwards on barcode level from the package id at the customer location to the barcodes at the Turning_Blank_Station.

# COMMAND ----------

production_process_df = spark.read.table("production_process_df").drop("part_number")
display(production_process_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating the production graph

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

# MAGIC %md
# MAGIC From the vertices and the edges data frames we can now define the graph

# COMMAND ----------

g = GraphFrame(vertices_df, edge_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The customer only provided the package id's. At the first glance, we do not know the effected plants. We do know that the first occurance of the package id is at the station 'Packaging_Logistics'. Let's search in the vertices for all possible nodes that relate to the given package id's.

# COMMAND ----------

error_type1_df = spark.read.table("error_type1_df")
start_search_nodes = (production_process_df.
                        select("plant").
                        distinct().
                        crossJoin(error_type1_df).
                        withColumn("SID", f.lit("Packaging_Logistics")).
                        withColumn("id", f.concat_ws('/',f.col("package_id"),f.col("SID"), f.col("plant"))).
                        join(g.vertices, "id").
                        select("id")
)
display(start_search_nodes)

# COMMAND ----------

# MAGIC %md
# MAGIC Using Breadth First Search, we can for example find the path from the related node with the SID
# MAGIC
# MAGIC
# MAGIC
# MAGIC   for the pathes from the first search node in the above list to the node with the SID 'Packaging_Logistics'

# COMMAND ----------

# Breadth firts search
example_path = g.bfs(
  fromExpr = "id = '" + start_search_nodes.collect()[0][0] + "'",
  toExpr = "SID = 'Turning_Blank_Station'",
  maxPathLength = 5)
display(example_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Since the production process of all parts has the same structure, this example provides the search architecture for a Motif search

# COMMAND ----------



# COMMAND ----------

cols = example_path.columns
cols[0] = "from"
cols[-1] = "to"
cols.reverse()
cols

# COMMAND ----------

path_structure_lst = ["(" + cols[i:(i+3)][0] + ")-[" + cols[i:(i+3)][1] + "]->(" + cols[i:(i+3)][2] + ")" for i in range(0, (len(cols) - 2), 2)]
motif_search_expression = ";".join(path_structure_lst)
motif_search_expression

# COMMAND ----------

in_expression = "to.id in ('" + "','".join(list(start_search_nodes.toPandas()["id"])) + "')"
in_expression

# COMMAND ----------

chain = g.find(motif_search_expression).filter(in_expression)
display(chain)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


