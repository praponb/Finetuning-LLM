from pyspark.sql import functions as F

rollup_totals = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.channel").alias("channel"), F.col("e.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.cost")).alias("min_cost"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select("channel", "rows_total", "min_cost", "gid")
    .orderBy(F.col("gid").desc_nulls_last(), "channel")
)
