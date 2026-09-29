from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 340) & F.col("product_id").isNotNull())
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .orderBy(F.col("revenue").desc())
)
