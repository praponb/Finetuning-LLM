from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("t.avg_unit_price"))
)
