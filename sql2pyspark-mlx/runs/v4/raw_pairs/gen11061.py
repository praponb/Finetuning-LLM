from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("amount") < 1380)
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.bank_transfer_count") >= 7)
    .select(F.col("o.id"), F.col("o.status"), F.col("os.bank_transfer_count"))
    .orderBy("status", "id")
    .limit(10)
)
