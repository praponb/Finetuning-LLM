from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["card", "paypal"]))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
    .filter(F.col("pct_bank_transfer") > 78)
    .orderBy("paid_quarter")
)
