from pyspark.sql import functions as F

by_dimension = (
    df_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["channel", "order_status"], ["channel"], []],
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.amount")).alias("max_amount"),
    )
)

result = (
    by_dimension
    .select(
        "channel",
        "order_status",
        "rows_total",
        "max_amount",
        F.when(F.col("order_status").rlike("^(RETURNED|CANCELLED)$"), "GROUP_B").otherwise("CORE").alias("order_status_zone"),
    )
)
