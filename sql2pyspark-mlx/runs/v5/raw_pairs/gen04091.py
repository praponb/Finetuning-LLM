from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") > 54)
)
