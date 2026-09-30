from pyspark.sql import functions as F

rollup_totals = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .rollup(F.col("o.channel").alias("channel"), F.col("s.method").alias("method"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.amount")), 2).alias("avg_amount"),
    )
)

result = (
    rollup_totals
    .select(
        "channel",
        "method",
        "num_rows",
        F.when(F.col("channel").rlike("^(store|partner)$"), "EXTENDED").when(F.col("channel").rlike("^(web|mobile)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy("method")
)
