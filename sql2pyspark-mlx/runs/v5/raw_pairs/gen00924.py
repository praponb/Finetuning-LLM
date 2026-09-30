from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("subscriptions.monthly_fee") > 140)
    .groupBy(F.month(F.col("customers.signup_date")).alias("signup_month"))
    .agg(
        F.round(F.avg(F.col("subscriptions.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc(), "signup_month")
    .limit(20)
)
