from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1040)
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "method",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 1280)
)
