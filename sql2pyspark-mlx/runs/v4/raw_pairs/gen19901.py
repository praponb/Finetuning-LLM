from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("p.price"),
        F.when(F.col("p.price") >= 1300, "High").when(F.col("p.price") >= 300, "Medium").otherwise("Low").alias("price_level"),
    )
)
