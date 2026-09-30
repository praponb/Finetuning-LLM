from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("quantity") < 5) & ((F.col("product_id") == 39) | F.col("product_id").isin([4, 25, 28, 29])))
)

summary = (
    src
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = summary.select("product_id", "quantity_bucket", "unit_price_range")
