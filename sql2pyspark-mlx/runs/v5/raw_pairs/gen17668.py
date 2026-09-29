from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.count("age").alias("age_count"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .filter(F.col("age_count") < 7)
    .orderBy("age_count")
)
