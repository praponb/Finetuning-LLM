from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.plan").isin(["basic", "premium"]) | (F.col("s.end_date") < "2025-10-13"))
    .select(F.col("s.status"), F.col("c.signup_date"), F.col("c.city"))
)
