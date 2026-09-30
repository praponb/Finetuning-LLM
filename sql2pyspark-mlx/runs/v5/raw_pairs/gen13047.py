from pyspark.sql import functions as F

result = (
    df_payments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.channel").alias("channel"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
