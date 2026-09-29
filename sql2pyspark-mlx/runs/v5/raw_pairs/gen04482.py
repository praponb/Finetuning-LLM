from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["order_status", "channel", "payment_status"], ["order_status", "channel"], ["order_status"]],
        F.col("p.status").alias("order_status"),
        F.col("p.channel").alias("channel"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.amount")).alias("max_amount"),
    )
)

result = (
    dim_totals
    .select(
        "order_status",
        "channel",
        "payment_status",
        "num_rows",
        "max_amount",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "GROUP_A").when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").asc_nulls_first())
)
