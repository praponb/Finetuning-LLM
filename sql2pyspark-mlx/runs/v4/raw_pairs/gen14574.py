from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | (F.col("unit_price") >= 210))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.countDistinct("unit_price").alias("unique_orders"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 1)
    .drop("quantity_count")
)
