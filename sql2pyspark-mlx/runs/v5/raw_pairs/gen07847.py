from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        "order_id",
        F.when(F.col("method").isin(["bank_transfer", "promptpay"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 2)
    .drop("amount_count")
    .orderBy(F.col("min_amount").desc(), "status", "order_id", "method_group")
    .limit(10)
)
