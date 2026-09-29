from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.quantity") == 8)
    .groupBy(F.col("o.store_id"))
    .agg(
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
    )
)
