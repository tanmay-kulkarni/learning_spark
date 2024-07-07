# Databricks notebook source
# MAGIC %run "./33_setup_data_sets_to_perform_joins"

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

users_df.join(course_enrolments_df, on='user_id', how='inner').show()

# COMMAND ----------

# the column on which the dataframes are joined get repeated in the result with this syntax

users_df.alias("u").join(
    course_enrolments_df.alias("ce"), users_df["user_id"] == course_enrolments_df["user_id"], how="inner"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join example
# MAGIC
# MAGIC * Find the total number of courses enrolled by each user

# COMMAND ----------

from pyspark.sql import functions as F

result_df = (
    users_df.alias("u")
    .join(course_enrolments_df.alias("ce"), on=["user_id"], how="left")
    .groupBy("u.user_id")
    .agg(
        F.sum(F.when(F.col("course_enrolment_id").isNull(), 0).otherwise(1)).alias(
            "courses_enrolled"
        )
    )
    .orderBy(F.desc("courses_enrolled"))
)

result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Another way of writing the same query

# COMMAND ----------

from pyspark.sql import functions as F

result_df = (
    users_df.alias("u")
    .join(course_enrolments_df.alias("ce"), on=["user_id"], how="left")
    .groupBy("u.user_id")
    .agg(
        F.sum(
            F.expr(
                """
                    CASE 
                        WHEN 
                            ce.course_enrolment_id is NULL 
                        THEN 0 
                        ELSE 1 
                    END
                """
            )
        ).alias("courses_enrolled")
    )
    .orderBy(F.desc("courses_enrolled"))
)

result_df.show()
