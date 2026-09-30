from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.country") != "JP")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc())
)
