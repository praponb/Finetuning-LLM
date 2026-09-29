from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .groupBy(F.col("c.country"), F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 3)
)
