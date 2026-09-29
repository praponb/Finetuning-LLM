from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("unit_price") <= 160).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.price"))
)
