from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .filter(F.col("order_stats.amount_range") >= 49)
    .select(
        F.col("orders.customer_id"),
        F.col("orders.discount"),
        F.col("order_stats.amount_range"),
    )
    .orderBy(F.col("customer_id").desc())
)
