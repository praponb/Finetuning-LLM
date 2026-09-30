from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country") == "UK")
    .select(F.col("s.end_date"), F.col("c.credit_limit"))
    .orderBy("credit_limit")
)
