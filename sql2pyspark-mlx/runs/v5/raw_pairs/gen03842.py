from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-07-01") & ~F.col("status").isin(["SUCCESS", "FAILED"]))
    .select("status", "amount")
    .orderBy(F.col("amount").desc())
)
