from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("unit_price") > 340)

order_items_stats = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    order_items_stats
    .select("order_id", "revenue")
    .orderBy(F.col("order_id").desc())
)
