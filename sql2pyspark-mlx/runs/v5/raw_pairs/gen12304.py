from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.year(F.col("o.order_date")) >= 2025) & F.col("oi.product_id").isin([5, 8, 11, 12]))
    .select(
        F.col("oi.unit_price"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
)
