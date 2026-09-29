from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .filter(F.col("payments.status").isin(["FAILED", "SUCCESS"]))
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum(F.col("payments.amount")).alias("total_amount"),
        F.count(F.when(F.col("payments.method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
    .orderBy("channel")
)
