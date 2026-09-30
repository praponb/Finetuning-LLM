from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.year(F.col("s.start_date")) >= 2023)
    .select(F.col("s.id"), F.col("s.monthly_fee"), F.col("s.plan"), F.col("c.segment"))
)
