from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        F.when(F.col("amount") >= 1230, "high").when(F.col("amount") >= 700, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)
