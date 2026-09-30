from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel").alias("channel"),
        F.col("e.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.amount")).alias("min_amount"),
    )
    .orderBy("channel")
)
