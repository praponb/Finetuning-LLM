from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 13) | F.col("quantity").isNull())
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .orderBy("unit_price_count", "unit_price_bucket")
    .limit(5)
)
