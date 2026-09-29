from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 11, 29]) | F.col("order_id").isin([23, 26]))
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") >= 5)
    .drop("unique_orders")
    .orderBy("quantity_bucket", "order_id")
    .limit(20)
)
