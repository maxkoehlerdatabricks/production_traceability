# Databricks notebook source
# MAGIC %md
# MAGIC This notebook was created using an ML enabled cluster of runtime 14.1 on an Azure Databricks workspace which is Unity Catalog enabled

# COMMAND ----------

# MAGIC %md
# MAGIC # Delivery Tracking

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Goal**: Trace forewards from a set of supplier barcodes to the delivery staus
# MAGIC
# MAGIC **Situation**:
# MAGIC - The supplier shipped several raw parts to the manufacturer and reports that some of these parts are suspicious
# MAGIC - The manufacturer wants to know in which products they are assembled and identify the package id's to either notify the customers or stop these products from being shipped to the customer
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
from graphframes.lib import *
AM = AggregateMessages
import networkx as nx
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we will do from a Graph perspective

# COMMAND ----------

# MAGIC %md
# MAGIC Say we have a simple graph

# COMMAND ----------

edges_example_df = sqlContext.createDataFrame([
  ("A1", "B", "some_metadata"),
  ("A2", "B", "some_metadata"),
  ("B", "C1", "some_metadata"),
  ("B", "C2", "some_metadata"),
  ], ["src", "dst", "Edge_Data"])

display(edges_example_df)

# COMMAND ----------

def create_vertices_from_edges(edges):
  vertices = ((edges.
   select(f.col('src')).
   distinct().
   withColumnRenamed('src','id')).
 union(
    (edges.
     select(f.col('dst')).
     distinct().
     withColumnRenamed('dst','id'))
 ).distinct()
 )
  return(vertices)

# COMMAND ----------

vertices_example_df = create_vertices_from_edges(edges_example_df)

display(vertices_example_df)

# COMMAND ----------

g_example = GraphFrame(vertices_example_df, edges_example_df)

# COMMAND ----------

#gnx_example = nx.from_pandas_edgelist(edges_example_df.toPandas(), source='src', target='dst', create_using=nx.DiGraph())
#nx.draw_spring(gnx_example, with_labels= True, font_size = 7, font_color = "red", node_color = "lightgrey")

# COMMAND ----------

# MAGIC %md
# MAGIC Say, we want to trace forwards from A to B and then from B to C. In general we want to trace forwards from a starting point until we reach a vertice with either zero outdegress or a specified condition. One way to do this is to apply aggreagted messaging https://graphframes.github.io/graphframes/docs/_site/user-guide.html#message-passing-via-aggregatemessages

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize

# COMMAND ----------

# We will update this graph object
gx = g_example

# Initiate Iteration
iteration = 1
  
# Initiate the edges
updated_edges = gx.edges.select("src","dst").withColumn("aggregated_parents", f.col("src"))
updated_edges = updated_edges.localCheckpoint()
 
# Initiate the vertices
updated_vertices = gx.vertices
updated_vertices = updated_vertices.localCheckpoint()

# Initiate the graph
g_for_loop = GraphFrame(updated_vertices, updated_edges)

# Initiate vertices_with_agg_messages
emptyRDD = spark.sparkContext.emptyRDD()
schema = StructType([
    StructField('id', StringType(), True),
    StructField('aggregated_parents', ArrayType(StringType(), True)),
    StructField('iteration', LongType(), True)
])
vertices_with_agg_messages = spark.createDataFrame(emptyRDD,schema)

#Aggregated Messaging
msgToDst = AM.edge["aggregated_parents"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### First iteration

# COMMAND ----------

# Aggregate messages
agg = g_for_loop.aggregateMessages(
  f.collect_set(AM.msg).alias("aggregated_parents"),
  sendToSrc=None,
  sendToDst=msgToDst
)

if (iteration > 1):
  agg = agg.withColumn("aggregated_parents",f.flatten(f.col("aggregated_parents")))

agg = agg.withColumn("iteration", f.lit(iteration))

display(agg)

# COMMAND ----------

# MAGIC %md 
# MAGIC Note that the vertice A does not appear any more, since it is the first vertice (zero in-degress) and there is nothing that could have been sent to this vertice

# COMMAND ----------

# Collect these data after each 
vertices_with_agg_messages = vertices_with_agg_messages.union(agg)
display(vertices_with_agg_messages)

# COMMAND ----------

# MAGIC %md
# MAGIC As we already sent data from A to B and collected the aggregated messages, we can subset the edge dataframe to those edges where we can possibly expect more aggregated messages

# COMMAND ----------

#Update edges
updated_edges = g_for_loop.edges
updated_edges = (updated_edges.
                 drop("aggregated_parents").
                 join(agg, updated_edges["src"] == agg["id"], how="inner").
                 select("src", "dst", "aggregated_parents")
)

display(updated_edges)

# COMMAND ----------

# Update the graph
g_for_loop = GraphFrame(updated_vertices, updated_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Second iteration

# COMMAND ----------

#Increase iteration
iteration+=1

# COMMAND ----------

# Aggregate messages
agg = g_for_loop.aggregateMessages(
  f.collect_set(AM.msg).alias("aggregated_parents"),
  sendToSrc=None,
  sendToDst=msgToDst
)

if (iteration > 1):
  agg = agg.withColumn("aggregated_parents",f.flatten(f.col("aggregated_parents")))

agg = agg.withColumn("iteration", f.lit(iteration))

display(agg)

# COMMAND ----------

# Collect these data after each 
vertices_with_agg_messages = vertices_with_agg_messages.union(agg)
display(vertices_with_agg_messages)

# COMMAND ----------

#Update edges
updated_edges = g_for_loop.edges
updated_edges = (updated_edges.
                 drop("aggregated_parents").
                 join(agg, updated_edges["src"] == agg["id"], how="inner").
                 select("src", "dst", "aggregated_parents")
)

display(updated_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC As this dataframe does not have any rows, we end our iterations at this point

# COMMAND ----------

# MAGIC %md
# MAGIC #### Collect the results

# COMMAND ----------

#Subset to final iteration per id
helper = vertices_with_agg_messages.groupBy("id").agg(f.max("iteration").alias("iteration"))
vertices_with_agg_messages = helper.join(vertices_with_agg_messages, ["id", "iteration"],  how="inner")
  
display(vertices_with_agg_messages)

# COMMAND ----------

# Subset to furthermost parents, the furthermost parents are those vertics that do not appear in the below table
out_degrees_df = gx.outDegrees
display(out_degrees_df)

# COMMAND ----------

furthermost_parents = gx.vertices.join(out_degrees_df, ["id"], how='left_anti' )
display(furthermost_parents)

# COMMAND ----------

# We can now easily create a table with the furthermost parents all pre-decessors
vertices_with_agg_messages = (furthermost_parents.
                              join(vertices_with_agg_messages, ["id"], how="inner").
                              select(f.col("id").alias("fin"), f.explode(f.col("aggregated_parents")).alias("raw"))
)
display(vertices_with_agg_messages)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wrap this into a function

# COMMAND ----------

def find_fin_for_raw(gx):

  ####################################################
  # Initialize
  ####################################################

  # Initiate Iteration
  iteration = 1
  
  # Initiate the edges
  updated_edges = gx.edges.select("src","dst").withColumn("aggregated_parents", f.col("src"))
  updated_edges = updated_edges.localCheckpoint()
 
  # Initiate the vertices
  updated_vertices = gx.vertices
  updated_vertices = updated_vertices.localCheckpoint()

  # Initiate the graph
  g_for_loop = GraphFrame(updated_vertices, updated_edges)

  # Initiate vertices_with_agg_messages
  emptyRDD = spark.sparkContext.emptyRDD()
  schema = StructType([
    StructField('id', StringType(), True),
    StructField('aggregated_parents', ArrayType(StringType(), True)),
    StructField('iteration', LongType(), True)
  ])
  vertices_with_agg_messages = spark.createDataFrame(emptyRDD,schema)

  #Aggregated Messaging
  msgToDst = AM.edge["aggregated_parents"]

  ####################################################
  # Iteratively pass furthermost children to parents
  ####################################################

  while(True):
    
    #############THE WHILE LOOP END HERE

    # Aggregate messages
    agg = g_for_loop.aggregateMessages(
      f.collect_set(AM.msg).alias("aggregated_parents"),
      sendToSrc=None,
      sendToDst=msgToDst
    )

    if (iteration > 1):
      agg = agg.withColumn("aggregated_parents",f.flatten(f.col("aggregated_parents")))

    agg = agg.withColumn("iteration", f.lit(iteration))

    # Collect these data after each
    vertices_with_agg_messages = vertices_with_agg_messages.union(agg)
    
    #Update edges
    updated_edges = g_for_loop.edges
    updated_edges = (updated_edges.
                 drop("aggregated_parents").
                 join(agg, updated_edges["src"] == agg["id"], how="inner").
                 select("src", "dst", "aggregated_parents")
    )

    #Formulate Break condition
    if (updated_edges.count()==0):
      break
    
    #Checkpoint
    updated_edges = updated_edges.localCheckpoint()

    #Vertices don't have do be updated, they just lose connection due to deleting relevant edges
    
    # Update the graph
    g_for_loop = GraphFrame(updated_vertices, updated_edges)

    # Increase iteration count
    iteration+=1
   
    #############THE WHILE LOOP END HERE

  #Subset to final iteration per id
  helper = vertices_with_agg_messages.groupBy("id").agg(f.max("iteration").alias("iteration"))
  vertices_with_agg_messages = helper.join(vertices_with_agg_messages, ["id", "iteration"],  how="inner")
  
  # Subset to furthermost parents, the furthermost parents are those vertics that do not appear in the below table
  out_degrees_df = gx.outDegrees

  # Get furthermos parents
  furthermost_parents = gx.vertices.join(out_degrees_df, ["id"], how='left_anti' )

  # We can now easily create a table with the furthermost parents all pre-decessors
  res = (furthermost_parents.
                              join(vertices_with_agg_messages, ["id"], how="inner").
                              select(f.col("id").alias("fin"), f.explode(f.col("aggregated_parents")).alias("raw"))
  )

  return(res)


# COMMAND ----------

# MAGIC %md 
# MAGIC And we can test the function

# COMMAND ----------

display(find_fin_for_raw(gx))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Back to the production graph example

# COMMAND ----------

# MAGIC %md
# MAGIC The production graph is

# COMMAND ----------

edge_df = spark.read.table("edge_df")
vertices_df = spark.read.table("vertices_df")
g = GraphFrame(vertices_df, edge_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's first apply the above algorithm for the larger production graph

# COMMAND ----------

fins_for_raw_df = find_fin_for_raw(g)
display(fins_for_raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Tracing package fins for the raws is a matter of filtering this table
