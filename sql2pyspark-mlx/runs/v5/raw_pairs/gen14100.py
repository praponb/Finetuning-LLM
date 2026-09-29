from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & F.col("start_date").between("2023-03-29", "2025-03-01"))
)

summary = (
    src
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = (
    summary
    .filter(F.col("avg_monthly_fee") < 29)
    .orderBy(F.col("avg_monthly_fee").desc(), "start_cohort")
    .limit(50)
)
