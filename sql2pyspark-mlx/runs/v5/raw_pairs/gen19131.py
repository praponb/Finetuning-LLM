from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .rollup(F.col("o.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
        F.min(F.col("src.unit_price")).alias("min_unit_price"),
    )
)
