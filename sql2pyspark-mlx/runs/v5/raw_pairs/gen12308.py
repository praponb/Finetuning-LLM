from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-04-01")
    .select(
        "id",
        "paid_at",
        "amount",
        "method",
        F.lower("method").alias("method_lower"),
    )
    .orderBy("amount", "paid_at")
)
