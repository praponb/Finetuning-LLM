from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isin([24, 36]))
    .select(F.col("s.id"), F.col("s.plan"), F.col("s.end_date"), F.col("c.age"))
)
