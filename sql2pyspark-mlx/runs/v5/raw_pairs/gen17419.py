from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") != "SG")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
    )
)
