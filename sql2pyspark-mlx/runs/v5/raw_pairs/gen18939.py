from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("p.brand"), F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
