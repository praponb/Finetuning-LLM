from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.quantity") >= 4)
    .groupBy(F.col("o.channel"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
    )
    .orderBy("channel", "unit_price_range")
    .limit(50)
)
