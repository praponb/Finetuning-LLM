from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.total_unit_price") > 1800)
    .select(F.col("p.id"), F.col("p.price"), F.col("t.total_unit_price"))
    .orderBy("price")
)
