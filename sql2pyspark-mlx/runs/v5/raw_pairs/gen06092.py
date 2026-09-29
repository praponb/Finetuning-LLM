from pyspark.sql import functions as F

result = (
    df_payments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.channel").alias("channel"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.amount")).alias("max_amount"),
    )
)
