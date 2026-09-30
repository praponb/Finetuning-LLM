from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
)
