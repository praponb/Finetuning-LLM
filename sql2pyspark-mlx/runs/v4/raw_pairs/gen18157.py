from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.avg("age").alias("avg_age"),
        F.count("*").alias("row_count"),
        F.count("age").alias("age_count"),
    )
    .orderBy(F.col("avg_age").desc())
)
