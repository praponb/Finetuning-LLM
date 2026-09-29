from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id") == 38)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.when(F.col("method").isin(["promptpay", "bank_transfer"]), "Tier 1").otherwise("Other").alias("method_group"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

result = (
    base
    .groupBy("order_id")
    .pivot("method", ["card", "paypal"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
