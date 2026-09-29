from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("%Carol%") & (F.col("s.end_date") <= "2025-01-01"))
    .select(F.col("s.monthly_fee"), F.col("s.start_date"), F.col("c.name"))
)
