from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("pbo.max_amount"),
    )
)
