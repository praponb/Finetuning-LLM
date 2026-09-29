from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.monthly_fee") < 150)
    .select(
        F.col("s.monthly_fee"),
        F.col("s.end_date"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.trim(F.col("c.country")).alias("clean_country"),
    )
)
