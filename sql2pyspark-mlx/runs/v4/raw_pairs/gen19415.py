from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id") == 15).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(F.array_contains(F.col("p.tags"), "bestseller"))
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.brand"))
)
