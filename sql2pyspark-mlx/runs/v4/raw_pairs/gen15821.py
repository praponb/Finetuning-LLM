from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
    .filter(F.col("expired_monthly_fee") >= 46)
)
