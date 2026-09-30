from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
    )
    .orderBy(F.col("pct_expired").desc())
)
