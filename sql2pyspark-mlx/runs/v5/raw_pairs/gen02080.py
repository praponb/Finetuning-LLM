from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
    )
    .orderBy(F.col("avg_monthly_fee").desc())
)
