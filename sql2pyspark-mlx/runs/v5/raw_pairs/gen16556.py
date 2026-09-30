from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

result = (
    valid
    .select("order_id", "status")
    .subtract(
        df_payments
        .filter(~F.col("method").isin(["cash", "card"]))
        .select("order_id", "status")
    )
)
