from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") < 640)
    .select(F.col("p.product_name"), F.col("oi.order_id"), F.col("oi.unit_price"))
)
