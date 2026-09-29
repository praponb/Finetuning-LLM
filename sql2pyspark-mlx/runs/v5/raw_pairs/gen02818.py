from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([22, 24]))
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
        "product_id",
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("num_order_items") >= 3)
    .drop("num_order_items")
    .orderBy(F.col("order_id").desc(), "unit_price_bucket", "product_id")
    .limit(5)
)
