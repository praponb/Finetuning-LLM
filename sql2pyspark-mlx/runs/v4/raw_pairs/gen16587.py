from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "status",
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
    )
    .filter(F.col("pct_paused") > 58)
    .select("status", "start_cohort", "pct_paused")
    .orderBy(F.col("status").desc())
)
