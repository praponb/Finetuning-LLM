from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["order_status", "channel"], ["order_status"], []],
        F.col("dim.status").alias("order_status"),
        F.col("dim.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.cost")).alias("min_cost"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("order_status").asc_nulls_first())
)
