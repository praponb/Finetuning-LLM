from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([29, 32, 33]))
    .groupBy(
        F.when(F.col("unit_price") >= 300, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy("unit_price_count", F.col("max_quantity").desc())
)
