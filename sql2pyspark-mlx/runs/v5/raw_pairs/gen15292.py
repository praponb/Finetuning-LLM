from pyspark.sql import functions as F

result = (
    df_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.channel").alias("channel"),
        F.col("d.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.amount")).alias("avg_amount"),
    )
)
