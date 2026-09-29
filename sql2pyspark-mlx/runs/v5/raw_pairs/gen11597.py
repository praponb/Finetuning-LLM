from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("method") == "bank_transfer")

payments_stats = (
    filtered_payments
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    payments_stats
    .filter(F.col("avg_amount") > 34)
    .orderBy("order_id", "avg_amount")
    .limit(10)
)
