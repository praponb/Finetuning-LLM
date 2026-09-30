from pyspark.sql import functions as F

result = (
    df_payments
    .filter((~F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & F.col("order_id").isNotNull()) | F.col("method").isNull())
    .select(
        "paid_at",
        F.weekofyear("paid_at").alias("paid_at_weekofyear"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
