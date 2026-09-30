from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("country", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
)
