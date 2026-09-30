from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .groupBy(F.col("c.segment"))
    .agg(
        F.max(F.col("s.end_date")).alias("latest_end_date"),
        F.min(F.col("s.start_date")).alias("first_start_date"),
    )
)
