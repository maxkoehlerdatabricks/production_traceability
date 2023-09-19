# Databricks notebook source
# MAGIC %md
# MAGIC # Trace backwards to supplier batches

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Goal**: Trace back from a bunch of erroneous end products to supplier batches
# MAGIC
# MAGIC **Situation**:
# MAGIC - Products were produced by the manufacturer and then shipped to the customer
# MAGIC - The customer observed several broken products and reports on the broken products
# MAGIC - Then manufacturer checks internal data and does not observe an abnormality
# MAGIC - The manufacturer wants to 
# MAGIC     - Trace back to the supplier to check if the erroneous parts can be assigned to one and the same supplier batch
# MAGIC
# MAGIC **Motivation**: Significantly reduce the non conformance costs of the manufacturer
# MAGIC
# MAGIC **Prerequisites**: Run the notebook *00_Introduction_And_Setup*

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Supplier_Manufacturer_Customer.png?raw=true" width=100%>

# COMMAND ----------

# MAGIC %run ./_resources/00_setup $reset_all_data=false

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *
from graphframes import *
import networkx as nx
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we will do from a Graph perspective

# COMMAND ----------

# MAGIC %md
# MAGIC Say we have a simple graph

# COMMAND ----------

vertices_example_df = sqlContext.createDataFrame([
  ("A1","some_metadata"),
  ("B1", "some_metadata"),
  ("C1", "some_metadata"),
  ("A2","some_metadata"),
  ("B2", "some_metadata"),
  ("C2", "some_metadata")
  ], ["id", "Vertices_Data"])

display(vertices_example_df)

# COMMAND ----------

edges_example_df = sqlContext.createDataFrame([
  ("A1", "B1", "some_metadata"),
  ("B1", "C1", "some_metadata"),
  ("A2", "B2", "some_metadata"),
  ("B2", "C2", "some_metadata"),
  ], ["src", "dst", "Edge_Data"])

display(edges_example_df)

# COMMAND ----------

g_example = GraphFrame(vertices_example_df, edges_example_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC From this graph it is easy to see that it can be decomposed in two weakly connected subgraphs

# COMMAND ----------

sc.setCheckpointDir("dbfs:/dbfs/tmp/checkpoints")
connected_components_example_df = g_example.connectedComponents()
display(connected_components_example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC If we select the vertices if each component we have a smaller graph. Let's for example select the first component...

# COMMAND ----------

first_compentent = connected_components_example_df.select("component").distinct().collect()[0][0]
subgraph_vertices_example_df = connected_components_example_df.filter(connected_components_example_df.component == first_compentent)
display(subgraph_vertices_example_df)


# COMMAND ----------

# MAGIC %md
# MAGIC The related edge data frame is

# COMMAND ----------

subgraph_edges_example_df = (
                      subgraph_vertices_example_df.
                      withColumnRenamed("id", "src").
                      join(edges_example_df, on = "src", how = "inner").
                      select(edges_example_df.columns + ["component"])
)

display(subgraph_edges_example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC If we transform these to be a Pandas dataframes

# COMMAND ----------

subgraph_edges_example_pdf = subgraph_edges_example_df.toPandas()
subgraph_vertices_example_pdf = subgraph_vertices_example_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC we can use the networkx package and perform the backwards traceability with an egograph

# COMMAND ----------

  gnx = nx.from_pandas_edgelist(subgraph_edges_example_pdf, source='src', target='dst', edge_attr = subgraph_edges_example_pdf.columns.tolist(), create_using=nx.DiGraph())

  # There should be only one search id
  search_id = [node for node in ["C1", "C2"] if gnx.has_node(node)][0]

  backwards_ego_graph = nx.ego_graph(gnx.reverse(), search_id, radius=100)
  backwards_ego_graph_edge_df = nx.to_pandas_edgelist(backwards_ego_graph)

  display(backwards_ego_graph_edge_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The rest of the solution is about parallelsing this using Pandas UDF, see https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Back to the production graph example

# COMMAND ----------

# MAGIC %md
# MAGIC The customer recalls the following barcodes

# COMMAND ----------

error_type2_df = spark.read.table("error_type2_df")
display(error_type2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The supplier reports on their barcodes (diffrenet from the customer's barcodes) along with batches

# COMMAND ----------

supplier_batch_df = spark.read.table("supplier_batch_df")
display(supplier_batch_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Our goal is to trace backwards from the customer (last node, station "At_Customer") to the supplier ("At_Raw")

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/maxkoehlerdatabricks/production_traceability/blob/main/pictures/Complete_Process.png?raw=true" width=100%>

# COMMAND ----------

# MAGIC %md
# MAGIC The customer only provided the package id's. At the first glance, we do not know the effected plants. We do know that the customer's package id's refer to barcodes at the station "At_Customer". Let's search in the vertices for all possible nodes that relate to the given package id's.

# COMMAND ----------

# MAGIC %md
# MAGIC First of all we create the production graph

# COMMAND ----------

edge_df = spark.read.table("edge_df")
vertices_df = spark.read.table("vertices_df")
g = GraphFrame(vertices_df, edge_df)

# COMMAND ----------

display(edge_df)

# COMMAND ----------

production_process_df = spark.read.table("production_process_df").drop("part_number")
start_search_nodes = (production_process_df.
                        select("plant").
                        distinct().
                        crossJoin(error_type2_df).
                        withColumn("SID", f.lit("At_Customer")).
                        withColumn("id", f.concat_ws('/',f.col("package_id"),f.col("SID"), f.col("plant"))).
                        join(g.vertices, "id").
                        select("id")
)
display(start_search_nodes)

# COMMAND ----------

# MAGIC %md
# MAGIC The apply the aformentioned strategy we decompose the graph into its weakly connected components. To do this, we have to set a checkpoint directory.

# COMMAND ----------

sc.setCheckpointDir("dbfs:/dbfs/tmp/checkpoints")
connected_components_df = g.connectedComponents()
display(connected_components_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now easily subset the relevant compentents

# COMMAND ----------

relevant_components = (connected_components_df.
                       join(start_search_nodes, on = "id", how="inner").
                       select("id", "component").
                       withColumnRenamed("id", "search_id_in_this_component").
                       distinct()
)
display(relevant_components)

# COMMAND ----------

# MAGIC %md
# MAGIC From there, we can subset the relevant vertices

# COMMAND ----------

relevnat_vertices = (
  connected_components_df.
  join(relevant_components, on = "component", how = "inner").
  select("id", "component", "search_id_in_this_component").  #up to this step we have all relevant id's for the selected components
  join(vertices_df, on = "id", how = "inner"). #which we can use to filter the vertices dataframe
  select(vertices_df.columns + ["component", "search_id_in_this_component"])
)

display(relevnat_vertices)

# COMMAND ----------

# MAGIC %md
# MAGIC It is straight forward to now filter the edge_df accordingly

# COMMAND ----------

relevnat_edges = (
  relevnat_vertices.
  select("id", "component","search_id_in_this_component").
  withColumnRenamed("id", "src").
  join(edge_df, on = "src", how = "inner").
  select(edge_df.columns + ["component", "search_id_in_this_component"])
)

display(relevnat_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC We now create a Python function to traverse the production graph backwards using egograph in networkx

# COMMAND ----------

def ego_graph_on_component(pdf: pd.DataFrame) -> pd.DataFrame:

  # Create the networkx graph
  gnx = nx.from_pandas_edgelist(pdf, source='src', target='dst', edge_attr = pdf.columns.tolist(), create_using=nx.DiGraph())

  # There should be only one search id
  search_id = pdf["search_id_in_this_component"][0]

  # Create the egograph on the reversed graph to traverse the graph backwards
  backwards_ego_graph = nx.ego_graph(gnx.reverse(), search_id, radius=100)
  backwards_ego_graph_edge_df = nx.to_pandas_edgelist(backwards_ego_graph)

  # Select the relevant columns and rows
  res = backwards_ego_graph_edge_df[backwards_ego_graph_edge_df["SID_Parent"]== "At_raw"][["SID_Parent","BC_Parent", "plant", "search_id_in_this_component" ]] 

  #Rename the columns
  res.rename(columns={"SID_Parent": "SID", "BC_Parent": "BC_At_Supplier"}, inplace=True)

  return res

# COMMAND ----------

# MAGIC %md
# MAGIC and test the function using the first component in the dataset

# COMMAND ----------

component_sub_set = relevnat_edges.select("component").collect()[0][0]
pdf = relevnat_edges.filter(relevnat_edges.component == component_sub_set).toPandas()
display(pdf)

# COMMAND ----------

display(ego_graph_on_component(pdf))

# COMMAND ----------

# MAGIC %md
# MAGIC In the next step we parallelize the function using a Pandas UDF. To do this we define the schema of the output dataframe

# COMMAND ----------

output_schema = StructType(
  [
    StructField('SID', StringType()),
    StructField('BC_At_Supplier', StringType()),
    StructField('plant', IntegerType()),
    StructField('search_id_in_this_component', StringType())
  ]
)

# COMMAND ----------

backwards_traceability_df = (
  relevnat_edges
    .groupBy("component")
    .applyInPandas(ego_graph_on_component, output_schema)
)
display(backwards_traceability_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Finding the relevant supplier batches is now a matter of a simple join

# COMMAND ----------

supplier_batch_df = spark.read.table("supplier_batch_df")
display(supplier_batch_df)

# COMMAND ----------

responsible_supplier_batches_df = (backwards_traceability_df.
                                   withColumnRenamed("BC_At_Supplier", "id").
                                   join(supplier_batch_df, on=["id", "plant"], how="inner")
                                   )
display(responsible_supplier_batches_df)

# COMMAND ----------

# MAGIC %md
# MAGIC More precisely, the following supplier batches are effected

# COMMAND ----------

display(responsible_supplier_batches_df.select("supplier_batch").distinct())

# COMMAND ----------


