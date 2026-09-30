from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("order_id").desc(), "unit_price_bucket")
    .limit(3)
)
