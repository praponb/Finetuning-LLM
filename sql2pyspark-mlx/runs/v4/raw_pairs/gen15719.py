from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1160, 1270) & (F.col("method") == "paypal"))
    .select(
        "amount",
        F.month("paid_at").alias("paid_at_month"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
    .orderBy(F.col("paid_at_month").desc())
)
