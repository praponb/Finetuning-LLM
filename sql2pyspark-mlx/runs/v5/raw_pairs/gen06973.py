from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 27) & F.col("product_id").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 450, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy("max_unit_price", F.col("unit_price_range").desc(), "unit_price_bucket")
    .limit(50)
)
