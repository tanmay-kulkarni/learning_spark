# Databricks notebook source
data = [
    {"col1": float('nan'), "col2": None, "col3": "NaN"},
    {"col1": None, "col2": float('nan'), "col3": "NaN"},
    {"col1": float('nan'), "col2": float('nan'), "col3": None},
    {"col1": 1.0, "col2": 2.0, "col3": "value"}
]

# Create a Spark DataFrame from the list of dictionaries
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql.functions import isnan

# COMMAND ----------

df.filter(isnan('col2') == True).show()

# COMMAND ----------

# with sql style syntax, no need to import isnan
df.filter("isnan(col2) = True").show()
