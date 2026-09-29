from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & F.col("revenue").isNotNull())
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy(F.col("revenue_count").desc())
)
