# Databricks notebook source
dbutils.widgets.dropdown('reset_all_data', 'false', ['true', 'false'], 'Reset all data')
dbutils.widgets.text('catalogName',  'prod_analytics_catalog_max_kohler' , 'Catalog Name')
dbutils.widgets.text('dbName',  'traceability' , 'Database Name')

# COMMAND ----------

print("Starting ./_resources/01_data_generator")

# COMMAND ----------

# MAGIC %pip install networkx

# COMMAND ----------

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import random
import string
import datetime
import math

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

number_of_parts_produced = 100
distributed_on_plants = 2
production_steps = ["Direct_Assembly","Equip_Raw","Mill","Turning_Blank"]
n_parts_with_error_1 = 10
n_supplier_batches = 3

# COMMAND ----------

catalogName = dbutils.widgets.get('catalogName') 
dbName = dbutils.widgets.get('dbName')
reset_all_data = dbutils.widgets.get('reset_all_data') == 'true'

# COMMAND ----------

print(catalogName)
print(dbName)
print(reset_all_data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate the Production Graph Draft

# COMMAND ----------

spark.sql(f"""USE CATALOG {catalogName}""")
spark.sql(f"""USE {dbName}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE process_graph_draft (BC_Parent STRING, SID_Parent STRING, BC_Child STRING, SID_Child STRING, Assembly_Type STRING, Start_Time STRING, End_Time STRING, Link_to_Data STRING);
# MAGIC INSERT INTO process_graph_draft
# MAGIC VALUES 
# MAGIC ('Package_1', 'At_raw', 'Blank_1', 'Turning_Blank_Station', 'Equip_Raw', '1', '2', 'Suplier_Data'),
# MAGIC ('Blank_1', 'Turning_Blank_Station', 'Intermediate_1', 'Mill_Station', 'Turning_Blank','3', '4', 'Test_Data_1'),
# MAGIC ('Intermediate_1', 'Mill_Station', 'Intermediate_2', 'Direct_Assembly_Station', 'Mill', '5', '6', ''),
# MAGIC ('Intermediate_2', 'Direct_Assembly_Station', 'Final', 'Packaging_Logistics', 'Direct_Assembly', '7', '8',  ''),
# MAGIC ('Final', 'Packaging_Logistics', 'Final_at_stock', 'Shipment_Logistics', 'Package', '9', '10', ''),
# MAGIC ('Final_at_stock', 'Shipment_Logistics', 'Package_on_truck', 'Delivery_Logistics', 'Shipment','11', '12', ''),
# MAGIC ('Package_on_truck', 'Delivery_Logistics', 'Package_at_customer_location', 'At_Customer', 'Delivery','13', '14', '');

# COMMAND ----------

sample_process_graph_df = spark.read.table("process_graph_draft")
display(sample_process_graph_df)

# COMMAND ----------

# Create a time stamp look up with an exemplary process cycle time
production_start = pd.Timestamp(year=pd.Timestamp.today().year, month=pd.Timestamp.today().month, day=pd.Timestamp.today().day, hour=6, second = 0, microsecond=0, nanosecond=0)
production_start = production_start -  pd.to_timedelta(7, unit='d') #go one week back

seconds_per_day = 24 * 60 * 60

data = [(1, production_start + datetime.timedelta(seconds=0)), # Start
        (2, production_start + datetime.timedelta(seconds=61)), # Equip_Raw
        (3, production_start + datetime.timedelta(seconds=(61 + 1))), # Change to next station
        (4, production_start + datetime.timedelta(seconds=(61 + 1 + 61))), # Turning_Blank
        (5, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1))), # Change to next station 
        (6, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81))), # Mill
        (7, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1))), # Change to next station 
        (8, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61))), # Direct_Assembly
        (9, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61 + 2))), # Change to next station 
        (10, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61 + 2 + 7.1*60))), # Package
        (11, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61 + 2 + 7.1*60 + 1))), # Change to next station 
        (12, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61 + 2 + 7.1*60 + 1 + 12.1*60 ))), # Shipment
        (13, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61 + 2 + 7.1*60 + 1 + 12.1*60 + 1))), # Change to next station 
        (14, production_start + datetime.timedelta(seconds=(61 + 1 + 61 + 1 + 81 + 1 + 61 + 2 + 7.1*60 + 1 + 12.1*60 + 1 + 1.09 * seconds_per_day))) # Delivery
        ]
time_look_up_df = spark.createDataFrame(pd.DataFrame(data, columns =["Time_Integer", "Time_Stamp"]))

display(time_look_up_df)

# COMMAND ----------

# Join the time look up table to the sample graph dara frame
sample_process_graph_df = (sample_process_graph_df.
       join(time_look_up_df, sample_process_graph_df.Start_Time ==  time_look_up_df.Time_Integer,  how="left").
       withColumn("Start_Time", f.col("Time_Stamp")).
       drop("Time_Integer", "Time_Stamp").
       join(time_look_up_df, sample_process_graph_df.End_Time ==  time_look_up_df.Time_Integer,  how="left").
       withColumn("End_Time", f.col("Time_Stamp")).
       drop("Time_Integer", "Time_Stamp")
       )
display(sample_process_graph_df)

# COMMAND ----------

# Draw the graph
G = nx.from_pandas_edgelist(sample_process_graph_df.toPandas(), source='SID_Parent', target='SID_Child', edge_attr='Assembly_Type', create_using=nx.DiGraph())
nx.draw_spring(G, with_labels= True, font_size = 7, font_color = "red", node_color = "lightgrey")

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate the production graph

# COMMAND ----------

# Helper Function for Barcode Generation
def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def id_sequence_generator(n, size):
  #random.seed(123)
  res = set()
  while True:
    res.add(id_generator(size=size))
    if len(res) >= n:
      break
  return res

# COMMAND ----------

bc_df = spark.createDataFrame(id_sequence_generator(n=number_of_parts_produced, size =12), StringType()).toDF("BC").withColumn("row_number", f.monotonically_increasing_id())
p_id_raw_df = spark.createDataFrame(id_sequence_generator(n=number_of_parts_produced, size =7), StringType()).toDF("P_ID_RAW").withColumn("row_number", f.monotonically_increasing_id())
p_id_fin_df = spark.createDataFrame(id_sequence_generator(n=number_of_parts_produced, size =7), StringType()).toDF("P_ID_FIN").withColumn("row_number", f.monotonically_increasing_id())

bc_df = bc_df.join(p_id_raw_df, on="row_number", how="inner").join(p_id_fin_df, on='row_number', how="inner").drop("row_number") 

w = Window().orderBy(f.lit('A'))

bc_df = bc_df.withColumn("part_number", f.row_number().over(w))

blocks = math.floor(bc_df.count() / distributed_on_plants) + 1

# Distribute all this over plants
bc_df = (bc_df.withColumn("blocks", f.lit(blocks)).
          withColumn("plant", f.ceil(f.col("part_number") / f.col("blocks"))).
          drop("blocks")
        )

w = Window().partitionBy("plant").orderBy("part_number")

bc_df = (bc_df.
         withColumn("part_number_per_plant", f.row_number().over(w)).
         withColumn("part_number", f.col("part_number_per_plant")).
         drop("part_number_per_plant")
)
display(bc_df)

# COMMAND ----------

# Calculate the minimum cycle time and add a couple of seconds to make sure the production line is not stuck
min_cycle_time = (sample_process_graph_df.
  filter(sample_process_graph_df.Assembly_Type.isin(production_steps)).
  withColumn("difference", f.col("End_Time").cast("long") - f.col("Start_Time").cast("long")).
  select(f.max(f.col("difference")).alias("max_cycle_time")).
  collect()[0][0]
  )

# COMMAND ----------

# Blow up the sample process graph with barcodes
production_process_df = sample_process_graph_df.crossJoin(bc_df)

assert production_process_df.count() /  sample_process_graph_df.count() ==  number_of_parts_produced, "Blowing up the sample process graph with a cross join did not work"

# Assign barcodes to BC_Parent via SID_Parent
production_process_df = (production_process_df.
                         withColumn("BC_Parent", 
                                    f.when( f.col("SID_Parent") == "At_raw", f.col("P_ID_RAW")).
                                      otherwise(   
                                              f.when( f.col("SID_Parent").isin([ "Packaging_Logistics","Shipment_Logistics", "Delivery_Logistics", "At_Customer"]), f.col("P_ID_FIN")).
                                                otherwise(
                                                  f.when( f.col("SID_Parent").isin([ "Turning_Blank_Station","Mill_Station", "Direct_Assembly_Station"]), f.col("BC")).
                                                  otherwise(f.lit("ERROR"))
                                                )
                                              )
                                     )
)

# Check that all BC_Parent's are assigned, i.e. there is no SID_Parent that was not accounted for
assert production_process_df.select("BC_Parent", "SID_Parent").filter(f.col("BC_Parent") ==  "ERROR").count() == 0, "Check the assignment of BC_Child via SID_Parent"

# Assign barcodes to BC_Child via SID_Child
production_process_df = (production_process_df.
                         withColumn("BC_Child", 
                                    f.when( f.col("SID_Child") == "At_raw", f.col("P_ID_RAW")).
                                      otherwise(   
                                              f.when( f.col("SID_Child").isin([ "Packaging_Logistics","Shipment_Logistics", "Delivery_Logistics", "At_Customer"]), f.col("P_ID_FIN")).
                                                otherwise(
                                                  f.when( f.col("SID_Child").isin([ "Turning_Blank_Station","Mill_Station", "Direct_Assembly_Station"]), f.col("BC")).
                                                  otherwise(f.lit("ERROR"))
                                                )
                                              )
                                     )
)

# Check that all BC_Child's are assigned, i.e. there is no SID_Child that was not accounted for
assert production_process_df.select("BC_Child", "SID_Child").filter(f.col("BC_Child") ==  "ERROR").count() == 0, "Check the assignment of BC_Parent via SID_Parent"

production_process_df = production_process_df.drop("BC", "P_ID_RAW", "P_ID_FIN")

display(production_process_df)


# COMMAND ----------

production_process_df = (
  production_process_df.
  withColumn("Start_Time", ( f.col("Start_Time").cast("long")  + f.col("part_number") * min_cycle_time).cast("timestamp")).
  withColumn("End_Time", ( f.col("End_Time").cast("long")  + f.col("part_number") * min_cycle_time).cast("timestamp"))
  )

display(production_process_df)

# COMMAND ----------

# Add some noise to the production times
production_process_df = (
  production_process_df.
  withColumn('rand', (((f.rand() - 0.5)/ (0.5 / 0.3)) * 1000).cast("int")).
  withColumn("Start_Time", ((f.col("Start_Time").cast("long") * 1000  + f.col("rand") ) / 1000).cast("timestamp")).
  withColumn('rand2', (((f.rand() - 0.5)/ (0.5 / 0.3)) * 1000).cast("int")).
  withColumn("End_Time", ((f.col("End_Time").cast("long") * 1000  + f.col("rand2") ) / 1000).cast("timestamp")).
  drop("rand", "rand2")
  )
display(production_process_df)

# COMMAND ----------

# Check the times inbetween the parts
##display(production_process_df.filter(f.col("Assembly_Type") == "Turning_Blank").orderBy(f.col("Start_Time")))

# COMMAND ----------

# Save the data as a delta table
production_process_df.write.mode("overwrite").saveAsTable("production_process_df")

# COMMAND ----------

# MAGIC %md
# MAGIC # Simulate the measurement data

# COMMAND ----------

  # Parameters
  rpm_max = 18000 # curve accelerates to max and then gos down back to zero
  total_periods = 50
  ramp_up_periods = 3
  ramp_down_periods = 3

# COMMAND ----------

def generate_measurement_turning_rpm_series(pdf: pd.DataFrame) -> pd.DataFrame:
  # This function generate the measurment series for rpm data for the turning machine

  # Start
  start_turning_curve = pdf[ "Start_Time" ][0]
  # Stop
  end_turnig_curve = pdf[ "End_Time" ][0]

  # List of equidistant times from start to stop
  time = list(pd.date_range(start=start_turning_curve, end=end_turnig_curve, periods=total_periods))

  # Measurement series
  ramp_up = [(rpm_max - 0) / ramp_up_periods * k for k in range(0,ramp_up_periods)]
  ramp_down = [rpm_max - (rpm_max - 0) / ramp_down_periods * k for k in range(1,(ramp_down_periods + 1))]
  process_in_action = [rpm_max] * (total_periods - ramp_up_periods - ramp_down_periods)
  rpm = ramp_up + process_in_action + ramp_down

  # Collect
  pdf_measurements = pd.DataFrame(list(zip(time, rpm)), columns =['time', 'rpm'])
  pdf_measurements["part_number"] = pdf[ "part_number" ][0]
  pdf_measurements["plant"] = pdf[ "plant" ][0]

  return pdf_measurements

# COMMAND ----------

production_process_df = spark.read.table("production_process_df")
display(production_process_df)

# COMMAND ----------

#Test
#pdf = production_process_df.filter( (f.col("SID_Child")  == "Mill_Station") & (f.col("SID_Parent")  == "Turning_Blank_Station") & (f.col("plant") == 1) & (f.col("part_number") == 1) ).#toPandas()
#generate_measurement_turning_rpm_series(pdf)

# COMMAND ----------

turning_schema = StructType(
  [
    StructField('time', TimestampType()),
    StructField('rpm', IntegerType()),
    StructField('part_number', IntegerType()),
    StructField('plant', IntegerType())
                
  ]
)

# COMMAND ----------

# Set config accordingly
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

# Prepare by filtering to turning production steps
df_turning_production_steps = production_process_df.filter( (f.col("SID_Child")  == "Mill_Station") & (f.col("SID_Parent")  == "Turning_Blank_Station"))

measurement_series_turning_rpm = df_turning_production_steps.groupBy("part_number", "plant").applyInPandas(generate_measurement_turning_rpm_series, turning_schema)

display(measurement_series_turning_rpm)

# COMMAND ----------

error_starting_from_part_number = random.randint(1,(measurement_series_turning_rpm.filter(f.col("plant") == 1).select(f.max(f.col("part_number"))).collect()[0][0] - n_parts_with_error_1))
error_1_df = spark.range(error_starting_from_part_number, error_starting_from_part_number + n_parts_with_error_1, 1).toDF("part_number").withColumn("error", f.lit("type1")).withColumn("plant", f.lit(1))
display(error_1_df)

# COMMAND ----------

measurement_series_turning_rpm = (
  measurement_series_turning_rpm.join(error_1_df, on = ["part_number", "plant"]   , how="left").
        withColumn("rpm_new", 
                   f.when( (f.col("rpm") == rpm_max) & (f.col("error") == "type1"), f.col("rpm") * 0.975).
                   otherwise(f.col("rpm"))).
        withColumn("rpm", f.col("rpm_new").cast("Integer")).
        drop("rpm_new", "error")
        )

display(measurement_series_turning_rpm)

# COMMAND ----------

# Add some noise to the measurement data
measurement_series_turning_rpm = (measurement_series_turning_rpm.
  withColumn("rand", ((f.rand() - 0.5) * 20).cast("int")).
  withColumn("rpm", f.when( f.col("rpm") > 0 , f.col("rpm") + f.col("rand")  ).otherwise(f.col("rpm"))).
  drop("rand")
  )
display(measurement_series_turning_rpm)

# COMMAND ----------

# Save the data
measurement_series_turning_rpm.write.mode("overwrite").saveAsTable("measurement_series_turning_rpm")

# COMMAND ----------

error_type1_df = (production_process_df.
        filter( (f.col("Assembly_Type") == "Package")).
        join(error_1_df, [ "part_number", "plant" ], how= "inner").
        select(f.col("BC_Child").alias("package_id")).
        distinct() 
        )

display(error_type1_df) 

# COMMAND ----------

# Save the data
error_type1_df.write.mode("overwrite").saveAsTable("error_type1_df")

# COMMAND ----------

# MAGIC %md
# MAGIC # Simulate the measurement data

# COMMAND ----------

display(production_process_df)

# COMMAND ----------

w = Window().partitionBy("plant").orderBy("part_number")

supplier_batch_df = (production_process_df.
        filter((f.col("SID_Parent") == "At_raw")).
        withColumn("part_number_per_plant", f.row_number().over(w)).
        select("BC_Parent", "Start_Time", "plant", "part_number_per_plant")

)


tmp = (supplier_batch_df.
        groupBy("plant").agg( f.max("part_number_per_plant").alias("total_parts_per_plant") ).
        withColumn( "break_batch_after", f.ceil( f.col("total_parts_per_plant") / f.lit(n_supplier_batches)))
)


supplier_batch_df  = (supplier_batch_df.
                      join(tmp, "plant", "left" ).
                      withColumn("supplier_batch",   f.ceil(f.col("part_number_per_plant") / f.col("break_batch_after"))).
                      select("plant", f.col("BC_parent").alias("id"), "supplier_batch")
)

display(supplier_batch_df)


# COMMAND ----------

# Save the data
supplier_batch_df.write.mode("overwrite").saveAsTable("supplier_batch_df")

# COMMAND ----------

# Get a couple of problematic BC's at the customer to trace nach to supplier
error_type2_df = (
  supplier_batch_df.
  filter( (f.col("supplier_batch") == 1) & (f.col("plant") == 1)). # filter a specific batch in a plant
  withColumnRenamed("id", "BC_Parent"). # prepare for join
  withColumn("SID_Parent", f.lit("At_raw")). # preprae for join
  select("plant", "BC_Parent", "SID_Parent"). # prepare for join
  join(production_process_df , ["plant", "BC_Parent", "SID_Parent"], how= "inner"). # filtering join to get the part_numbers
  select("part_number", "plant"). # pepare for join
  join(production_process_df , ["part_number", "plant"], how= "inner").# filtering join to subset the part numbers
  filter(f.col("SID_Child") == "At_Customer"). # Get the respective barcodes at the customer
  select(f.col("BC_parent").alias("package_id")).
  distinct()
)

display(error_type2_df)

# COMMAND ----------

# Save the data
error_type2_df.write.mode("overwrite").saveAsTable("error_type2_df")

# COMMAND ----------


