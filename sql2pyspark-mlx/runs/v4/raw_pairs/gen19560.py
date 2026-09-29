from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "bank_transfer", "card"]))
    .select("method", "status")
    .unionByName(
        df_payments
        .filter(F.col("paid_at") < "2024-11-01")
        .select("method", "status")
    )
    .distinct()
)
