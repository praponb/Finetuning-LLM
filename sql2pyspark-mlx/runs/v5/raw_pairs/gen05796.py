from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("o.channel"), F.month(F.col("o.order_date")).alias("order_month"))
    .agg(
        F.count(F.col("oi.quantity")).alias("quantity_count"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
)
