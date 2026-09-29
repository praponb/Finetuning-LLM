from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-11-01")
    .select("id", "status", F.round("amount", 0).alias("amount_rounded"))
)
