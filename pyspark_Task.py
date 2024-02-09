# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing and installing required libraries and functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %pip install folium

# COMMAND ----------

import folium
from folium.plugins import MarkerCluster

# COMMAND ----------

# DBTITLE 1,Reading given Dataset
df = spark.read.option('header',True).csv('dbfs:/FileStore/csv_datasets/database.csv',)

# COMMAND ----------

# DBTITLE 1,Analysing the raw data
df.display()

# COMMAND ----------

# DBTITLE 1,Casting columns as per requirment
column_datatypes = {
    "Date": "string",
    "Time": "string",
    "Latitude": "float",
    "Longitude": "float",
    "Type": "string",
    "Depth": "float",
    "Magnitude": "float"
}
for column_name, data_type in column_datatypes.items():
    df = df.withColumn(column_name, col(column_name).cast(data_type))

df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "dd/MM/yyyy HH:mm:ss"))

df = df.drop("Date", "Time")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations

# COMMAND ----------

# DBTITLE 1,Filtering data by magnitude column
filtered_df = df.filter(col('Magnitude') > 5.0)

# COMMAND ----------

# DBTITLE 1,Calculating average of Depth and Magnitude column by earthquack type
average_stats_df = filtered_df.groupBy("Type").agg(avg("Depth").alias("Avg_Depth"), avg("Magnitude").alias("Avg_Magnitude"))

# COMMAND ----------

average_stats_df.display()

# COMMAND ----------

# DBTITLE 1,Registering UDF
# Defining a UDF to categorize earthquakes based on magnitude
@udf
def categorize_earthquake(magnitude):
    if magnitude >= 8:
        return "High"
    elif magnitude >= 7 and magnitude < 8:
        return "Moderate"
    else:
        return "Low"

# COMMAND ----------

# DBTITLE 1,Calling the UDF
categorized_df = filtered_df.withColumn("category", categorize_earthquake(df["Magnitude"]))

# COMMAND ----------

categorized_df.display()

# COMMAND ----------

# DBTITLE 1,Calculating Distance
reference_location = (0, 0)
distance_df = (
    categorized_df.withColumn(
        "Distance",
        sqrt((col("Latitude") - lit(reference_location[0])) ** 2 + (col("Longitude") - lit(reference_location[1])) ** 2)
    )
)

# COMMAND ----------

distance_df.display()

# COMMAND ----------

# DBTITLE 1,saving final output 
distance_df.write.format('csv').mode('overwrite').save('dbfs:/FileStore/csv_datasets/final_output.csv')

# COMMAND ----------

# DBTITLE 1,For Visualizing Data
map_center = [0, 0] 
world_map = folium.Map(location=map_center, zoom_start=2)

# COMMAND ----------

earthquake_locations = distance_df.select('Latitude', 'Longitude', 'Magnitude').collect()

# COMMAND ----------

# DBTITLE 1,marking on map
marker_cluster = MarkerCluster().add_to(world_map)
for location in earthquake_locations:
    folium.Marker([location['Latitude'], location['Longitude']],
                  popup=f"Magnitude: {location['Magnitude']}",
                  icon=None).add_to(marker_cluster)

# COMMAND ----------

# DBTITLE 1,saving the map in HTML format
world_map.save("earthquake_map.html")

# COMMAND ----------

display(world_map)
