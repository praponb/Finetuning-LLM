from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .rollup(F.col("o.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.unit_price")).alias("max_unit_price"),
        F.min(F.col("e.quantity")).alias("min_quantity"),
    )
    .orderBy("min_quantity")
)
