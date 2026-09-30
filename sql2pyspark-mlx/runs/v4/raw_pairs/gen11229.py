from pyspark.sql import functions as F

result = (
    df_order_items
    .rollup(
        "product_id",
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
)
