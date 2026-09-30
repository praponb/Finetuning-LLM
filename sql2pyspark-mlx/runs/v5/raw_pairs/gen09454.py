from pyspark.sql import functions as F

base = df_order_items.filter(F.col("unit_price").between(170, 240))

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    order_items_stats
    .filter(F.col("revenue") >= 76)
    .select("quantity_bucket", "order_id", "revenue")
    .orderBy("quantity_bucket")
)
