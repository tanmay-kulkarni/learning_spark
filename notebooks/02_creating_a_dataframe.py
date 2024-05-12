# Databricks notebook source
# MAGIC %md
# MAGIC **Type of variable `spark`**

# COMMAND ----------

type(spark)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

df = spark.createDataFrame([1,2,3], 'int')

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StringType
df = spark.createDataFrame([1,2,3], StringType())
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a dataframe using a list of tuples

# COMMAND ----------

users = [(1, 'Tanmay'), (2, 'Roger'), (3, 'David'), (4, 'Emily')]
df = spark.createDataFrame(users, 'user_id int, user_name string')
df.show()

# COMMAND ----------

df.collect(), type(df.collect()[1])

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

row1 = Row(name='Tanmay', age=31)

# COMMAND ----------

row1['name'], row1.name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a dataframe using a list of lists

# COMMAND ----------

users = [[1, 'Tanmay'], [2, 'Roger'], [3, 'Scott'], [4, 'Phil'], [5, 'Sarah']]

from pyspark.sql import Row

users_as_Row = [Row(*user) for user in users]

df = spark.createDataFrame(users_as_Row, 'user_id int, user_name string')

df.show()
