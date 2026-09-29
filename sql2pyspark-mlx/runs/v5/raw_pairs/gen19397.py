from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-01")
    .groupBy("method")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") < 61)
    .select("method", "amount_range")
    .orderBy("method")
)
