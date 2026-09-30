from pyspark.sql import functions as F

by_dimension = (
    df_payments
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.status").alias("order_status"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.amount")).alias("max_amount"),
    )
)

result = (
    by_dimension
    .select("order_status", "payment_status", "max_amount")
    .orderBy("order_status", "max_amount")
)
