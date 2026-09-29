from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "cash", "promptpay"]))
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "left")
    .filter(F.col("P.total_amount") > 5700)
    .select(
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("P.total_amount"),
        F.col("P.amount_count"),
    )
)
