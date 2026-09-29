from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(200, 240) & F.col("customer_id").isNull() & F.col("customer_id").isin([5, 15, 26, 39]))
    .select("id", "plan")
)
