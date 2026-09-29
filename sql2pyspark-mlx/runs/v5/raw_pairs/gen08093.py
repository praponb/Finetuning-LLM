from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("max_monthly_fee") > 150)
)
