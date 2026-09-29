from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "left")
    .select(F.col("o.customer_id"), F.col("P.success_count"), F.col("P.failed_amount"))
)
