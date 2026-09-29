from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.max(F.col("s.end_date")).alias("last_end_date"))
)
