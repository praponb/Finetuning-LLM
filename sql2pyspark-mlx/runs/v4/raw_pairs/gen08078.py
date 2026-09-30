from pyspark.sql import functions as F

max_monthly_fee = df_subscriptions.agg(F.max("monthly_fee")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > max_monthly_fee)
    .select("id", "monthly_fee", "customer_id", "status")
    .orderBy(F.col("customer_id").desc(), F.col("id").desc())
)
