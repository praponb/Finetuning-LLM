from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .orderBy("unit_price_bucket")
)
