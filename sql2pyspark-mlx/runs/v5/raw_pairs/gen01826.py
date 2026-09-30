from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "paid_at",
        "amount",
        "method",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
