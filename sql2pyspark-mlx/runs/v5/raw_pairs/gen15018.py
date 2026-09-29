from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 31) & (F.col("method") == "card"))
    .groupBy(
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        F.when(F.col("amount") >= 1440, "high").when(F.col("amount") >= 730, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.count("amount").alias("amount_count"),
    )
)
