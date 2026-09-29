from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .groupBy(F.col("o.channel"))
    .agg(
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
        F.count("*").alias("order_item_count"),
    )
    .filter(F.col("quantity_range") >= 47)
    .orderBy(F.col("channel").desc())
)
