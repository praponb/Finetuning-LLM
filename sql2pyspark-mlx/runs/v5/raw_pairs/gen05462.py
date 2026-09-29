from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") == "Chiang Mai")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
)
