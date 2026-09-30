from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["channel", "payment_status"], ["channel"]],
        F.col("d.channel").alias("channel"),
        F.col("e.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.amount")).alias("min_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("min_amount").desc_nulls_last())
)
