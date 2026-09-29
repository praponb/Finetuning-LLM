from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") > 3)
    .drop("age_count")
)
