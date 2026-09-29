from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 5, 7]) & F.col("store_id").isin([2, 4, 6]) & (F.col("product_id") == 3))
    .groupBy(
        F.when(F.col("revenue") >= 4000, "high").when(F.col("revenue") >= 1300, "mid").otherwise("low").alias("revenue_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .orderBy(F.col("product_id").desc())
)
