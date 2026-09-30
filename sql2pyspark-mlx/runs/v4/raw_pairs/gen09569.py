from pyspark.sql import functions as F

src = (
    df_payments
    .filter((F.col("status") != "FAILED") & (F.to_date("paid_at") < "2024-07-01") & (F.col("status") != "REFUNDED"))
)

payments_stats = (
    src
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    payments_stats
    .filter(F.col("amount_range") >= 26)
    .select("order_id", "amount_range")
    .orderBy(F.col("order_id").desc(), "amount_range")
)
