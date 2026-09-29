from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1550, "high").when(F.col("amount") >= 480, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .filter(F.col("failed_count") >= 4)
    .orderBy("min_paid_at", "amount_bucket", "paid_cohort")
    .limit(5)
)
