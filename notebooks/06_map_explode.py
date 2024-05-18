# Databricks notebook source
people = [
    {
        "name": "John Doe",
        "age": 35,
        "telephone": {"home": "555-1234", "office": "555-5678"},
    },
    {
        "name": "Jane Smith",
        "age": 28,
        "telephone": {"home": "555-9012", "office": "555-3456"},
    },
    {
        "name": "Bob Johnson",
        "age": 42,
        "telephone": {"home": "555-7890", "office": "555-2109"},
    },
    {"name": "David Brenson", "age": 23, "telephone": {"home": None, "office": None}},
    {"name": "George Sacks", "age": 36, "telephone": {"home": None}},
]

from pyspark.sql import Row

people_as_rows = [Row(**person) for person in people]

df = spark.createDataFrame(people_as_rows)

df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type map

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting value from a map

# COMMAND ----------

from pyspark.sql.functions import col

df.select(
    "name",
    col("telephone")["home"].alias("home_phone"),
    col("telephone")["office"].alias("office_phone"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### explode

# COMMAND ----------

from pyspark.sql.functions import explode, explode_outer

df.select('name', explode('telephone')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### explode_outer

# COMMAND ----------

df.select('name', explode_outer('telephone')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the explode key and values

# COMMAND ----------

df.select("name", explode("telephone")).withColumnsRenamed(
    {"key": "phone_type", "value": "number"}
).show()
