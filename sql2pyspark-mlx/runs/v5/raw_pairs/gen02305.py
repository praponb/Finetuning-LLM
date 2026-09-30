from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("signup_date").between("2024-09-16", "2025-03-25")).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("subscriptions.end_date") > "2024-01-23")
    .select(F.col("subscriptions.id"), F.col("subscriptions.end_date"))
)
