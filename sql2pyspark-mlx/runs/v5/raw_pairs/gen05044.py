from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.col("subscriptions.status") == "EXPIRED")
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(F.max(F.col("subscriptions.monthly_fee")).alias("max_monthly_fee"))
    .orderBy(F.col("max_monthly_fee").desc(), "signup_quarter")
    .limit(20)
)
