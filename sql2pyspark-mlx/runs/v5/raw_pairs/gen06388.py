from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"), F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"))
)
