from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .cube(F.col("p.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.quantity")).alias("total_quantity"),
        F.avg(F.col("s.unit_price")).alias("avg_unit_price"),
    )
)
