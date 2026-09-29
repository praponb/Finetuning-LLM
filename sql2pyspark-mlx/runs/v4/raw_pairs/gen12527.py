from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.unit_price").isNotNull())
    .groupBy(
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
        F.col("p.category"),
    )
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
)
