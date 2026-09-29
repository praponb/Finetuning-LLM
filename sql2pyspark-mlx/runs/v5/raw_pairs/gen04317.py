from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy("unique_statuss")
)
