from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"))
    .agg(F.max(F.col("s.start_date")).alias("last_start_date"))
    .orderBy(F.col("last_start_date").desc())
)
