from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 14]))
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") >= 1)
    .orderBy(F.col("unit_price_bucket").desc())
)
