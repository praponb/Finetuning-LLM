from pyspark.sql import functions as F

valid_order_items = df_order_items.filter((F.col("unit_price") >= 260) & (F.col("unit_price") > 310))

agg = (
    valid_order_items
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    agg
    .filter(F.col("avg_quantity") >= 6.2)
    .select("order_id", "avg_quantity", "revenue")
)
