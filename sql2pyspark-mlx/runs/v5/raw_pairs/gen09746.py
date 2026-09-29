from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 9400)
    .groupBy("city")
    .agg(
        F.max("age").alias("max_age"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .orderBy("stddev_age", F.col("max_age").desc())
)
