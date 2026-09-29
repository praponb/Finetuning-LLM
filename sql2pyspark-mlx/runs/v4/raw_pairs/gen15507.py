from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") == 7) & (F.col("unit_price") > 290))
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(
        F.col("min_quantity").desc(),
        F.col("total_count").desc(),
        "unit_price_bucket",
    )
    .limit(10)
)
