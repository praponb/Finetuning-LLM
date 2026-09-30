from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.min("start_date").alias("min_start_date"),
    )
    .filter(F.col("max_monthly_fee") < 160)
)
