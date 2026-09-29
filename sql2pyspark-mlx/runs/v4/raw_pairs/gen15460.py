from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.quantity") != 8)
    .select(F.col("p.price"), F.col("p.brand"), F.col("oi.product_id"))
    .orderBy("price")
)
