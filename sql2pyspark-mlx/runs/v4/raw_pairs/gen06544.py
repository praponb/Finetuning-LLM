from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.count("*").alias("num_store_sales"),
    )
    .orderBy(F.col("num_store_sales").desc())
)
