from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter((F.col("order_id") == 28) & F.col("product_id").isNull())
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

order_items_stats = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity_rounded") >= 88, "high").when(F.col("quantity_rounded") >= 31, "mid").otherwise("low").alias("quantity_rounded_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    order_items_stats
    .select("quantity_rounded_bucket", "avg_quantity")
    .orderBy("quantity_rounded_bucket")
)
