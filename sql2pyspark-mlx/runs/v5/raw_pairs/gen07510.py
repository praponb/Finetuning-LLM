from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 7) & F.col("order_id").isin([12, 16, 39]) & (F.col("unit_price") >= 300))
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.countDistinct("quantity").alias("unique_products"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("unique_orders") > 3)
    .orderBy(F.col("order_id").desc())
)
