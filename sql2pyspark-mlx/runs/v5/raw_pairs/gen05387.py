from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(170, 180) & (F.col("customer_id") == 5))
    .select("id", "customer_id", "start_date", "plan")
    .orderBy(F.col("id").desc())
    .limit(5)
)
