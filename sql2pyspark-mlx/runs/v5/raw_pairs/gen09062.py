from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
)
