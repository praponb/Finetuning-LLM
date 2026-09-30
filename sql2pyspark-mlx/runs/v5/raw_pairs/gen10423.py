from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .cube(
        F.col("d.channel").alias("channel"),
        F.col("e.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.amount")).alias("max_amount"),
        F.grouping_id().alias("gid"),
    )
)
