from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("unit_price").alias("unique_orders"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy("unit_price_bucket")
    .limit(50)
)
