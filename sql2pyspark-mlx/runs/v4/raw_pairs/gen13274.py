from pyspark.sql import functions as F

base = df_payments.filter(F.to_date("paid_at") >= "2024-08-01")

agg = (
    base
    .groupBy(
        "method",
        F.when(F.col("method").isin(["bank_transfer", "paypal"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    agg
    .filter(F.col("avg_amount") < 1360)
    .select("method", "method_group", "amount_count", "avg_amount")
)
