from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([33, 36]))
    .select("id", "monthly_fee", "start_date", "status")
    .orderBy(F.col("monthly_fee").desc())
)
