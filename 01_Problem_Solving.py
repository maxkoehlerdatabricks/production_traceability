# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Problem Solving with Traceability

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Goal**: Trace back from a bunch of erroneous end products to test data in production
# MAGIC
# MAGIC **Situation**:
# MAGIC - Products were produced by the manufacturer and then shipped to the customer
# MAGIC - The customer observed that the diameter of the end product is slightly too large and further assembly is at risk
# MAGIC - As a significant amount of products is affected, the customer recalls a complete time range
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
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Supplier_Manufacturer_Customer.png?raw=true" width=100%>

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
# MAGIC   - The measuremnt data is just an "endless" time seires and we do not know what part of the time seires to consider for which barcode
# MAGIC - If our analyses reveils a deficiency in production that completely explains the issue, we can
# MAGIC   - Gain trust bei explaining the issue to the customer
# MAGIC   - Identify the effected barcodes
# MAGIC   - Reduce the number of barcodes that are effected by the recall from the complete time range to a small list

# COMMAND ----------

# MAGIC %md
# MAGIC In the first step we create the production graph

# COMMAND ----------

edge_df = spark.read.table("edge_df")
vertices_df = spark.read.table("vertices_df")
g = GraphFrame(vertices_df, edge_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Our goal is to trace backwards from the customer (last node, station "At_Customer") to the station "Turning_Blank_Station".

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Complete_Process.png?raw=true" width=100%>

# COMMAND ----------

# MAGIC %md
# MAGIC The customer only provided the package id's. At the first glance, we do not know the effected plants. We do know that the first occurance of the package id is at the station 'Packaging_Logistics'. Let's search in the vertices for all possible nodes that relate to the given package id's.

# COMMAND ----------

production_process_df = spark.read.table("production_process_df").drop("part_number")
error_type1_df = spark.read.table("error_type1_df")
start_search_nodes = (production_process_df.
                        select("plant").
                        distinct().
                        crossJoin(error_type1_df).
                        withColumn("SID", f.lit("At_Customer")).
                        withColumn("id", f.concat_ws('/',f.col("package_id"),f.col("SID"), f.col("plant"))).
                        join(g.vertices, "id").
                        select("id")
)
display(start_search_nodes)

# COMMAND ----------

# MAGIC %md
# MAGIC Using Breadth First Search, we can for ian example path to go from "At_Customer" to "Turning_Blank_Station". 

# COMMAND ----------

# Breadth firts search
example_path = g.bfs(
  fromExpr = "id = '" + start_search_nodes.collect()[0][0] + "'",
  toExpr = "SID = 'Turning_Blank_Station'",
  maxPathLength = 10)
display(example_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Since the production process of all parts has the same structure, this example provides the search architecture for a Motif search

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

# MAGIC %md
# MAGIC Now, we can select the information of interest

# COMMAND ----------

traceability = (chain.
                withColumn("plant", f.col("to.plant")).
                withColumn("ID_at_Customer", f.col("to.BC")).
                withColumn("ID_at_Turning_Blank_Station", f.col("from.BC")).
                withColumn("Start_Turning_Blank", f.col("e0.Start_Time")).
                withColumn("End_Turning_Blank", f.col("e0.End_Time")).
                select("plant", "ID_at_Customer", "ID_at_Turning_Blank_Station", "Start_Turning_Blank", "End_Turning_Blank")
               )
display(traceability)

# COMMAND ----------

# MAGIC %md
# MAGIC This puts us in the position to indentfy the parts of the "endless" time series at which the problematic parts were produced. 

# COMMAND ----------

measurement_series_turning_rpm = spark.read.table("measurement_series_turning_rpm")
display(measurement_series_turning_rpm)


# COMMAND ----------

suspicious_series = (traceability.
                     join(measurement_series_turning_rpm, (traceability.plant == measurement_series_turning_rpm.plant) & (traceability.Start_Turning_Blank <= measurement_series_turning_rpm.time) & (traceability.End_Turning_Blank >= measurement_series_turning_rpm.time)).
                     select(traceability.plant, traceability.ID_at_Customer, traceability.ID_at_Turning_Blank_Station, measurement_series_turning_rpm.time, measurement_series_turning_rpm.rpm)
                     )

display(suspicious_series)

# COMMAND ----------

# MAGIC %md
# MAGIC If we look at on of those series we see that the plateau of this measuremnet series is slightly below the specification of 18k round per minute

# COMMAND ----------

filter_part = suspicious_series.select(f.col("ID_at_Customer")).collect()[0][0]
sample_suspicious_part = suspicious_series.filter(f.col("ID_at_Customer") == filter_part)
display(sample_suspicious_part)

# COMMAND ----------

# MAGIC %md
# MAGIC This seems to hold true for all parts that were recalled

# COMMAND ----------

display(suspicious_series.groupBy("ID_at_Customer").agg(f.max("rpm")))

# COMMAND ----------

# MAGIC %md
# MAGIC When looking at all parts, we see that the max is somewhere just above the specification

# COMMAND ----------

display(measurement_series_turning_rpm.groupBy("part_number").agg(f.max("rpm")))

# COMMAND ----------

# MAGIC  %md
# MAGIC  After conducting a physical simulation we can now prove that the slightly increased diameter of the end product is due to a slightly too slow turning process. We can identify all problematic parts that are shipped to the customer and explain the issue to the customer. This significantly reduces the number of affected products by the recall. Furthermore, implementing a preventive action is straightforward, since we only need to adapt respective thresholds in the inline measurement system.
