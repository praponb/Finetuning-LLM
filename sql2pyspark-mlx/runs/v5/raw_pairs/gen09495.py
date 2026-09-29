from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price") > 140)
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_quantity") >= 3.3)
    .select(F.col("p.id"), F.col("p.price"), F.col("t.avg_quantity"))
)
