from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-09-01")
    .select(
        "id",
        "amount",
        "paid_at",
        "method",
        F.month("paid_at").alias("paid_at_month"),
    )
    .orderBy("method")
)
