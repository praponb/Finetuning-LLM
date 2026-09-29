from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .rollup(
        F.col("o.channel").alias("channel"),
        F.col("o.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.cost")).alias("max_cost"),
        F.grouping_id().alias("level_id"),
    )
)
