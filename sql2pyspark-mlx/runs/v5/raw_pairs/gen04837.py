from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("min_quantity") >= 3.0)
)
