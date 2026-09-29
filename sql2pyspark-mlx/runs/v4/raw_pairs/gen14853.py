from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("order_id").isNotNull() & F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"])))
    .select("id", "paid_at", F.substring("status", 1, 2).alias("status_prefix"))
)
