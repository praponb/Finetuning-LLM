from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .groupBy(
        F.when(F.col("method").isin(["paypal", "card"]), "tier_1").otherwise("other").alias("method_group"),
        "method",
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .orderBy(F.col("method").desc(), "method_group")
    .limit(5)
)
