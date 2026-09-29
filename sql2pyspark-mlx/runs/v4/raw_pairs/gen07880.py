from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.start_date") <= "2025-03-13")
    .select(F.col("c.age"), F.col("s.monthly_fee"), F.col("s.plan"))
)
