from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise", "premium"]) & F.col("customer_id").isNull())
    .select("id", "monthly_fee", "plan", "customer_id")
    .orderBy(F.col("id").desc(), F.col("monthly_fee").desc())
)
