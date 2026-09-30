from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("amount") > 950)
    .withColumn("amount_whole", F.ceil("amount"))
)

summary = (
    recent_payments
    .groupBy(
        "status",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    summary
    .select("status", "paid_cohort", "first_paid_at")
    .orderBy(F.col("first_paid_at").desc(), "paid_cohort", "status")
    .limit(5)
)
