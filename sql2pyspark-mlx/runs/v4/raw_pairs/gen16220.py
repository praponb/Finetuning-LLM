from pyspark.sql import functions as F

src = df_order_items.filter(F.col("product_id").isNotNull())

order_items_stats = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    order_items_stats
    .filter(F.col("total_count") < 7)
    .orderBy(F.col("quantity_bucket").desc())
)
