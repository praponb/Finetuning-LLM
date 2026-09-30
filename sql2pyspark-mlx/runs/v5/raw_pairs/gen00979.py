from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
)
