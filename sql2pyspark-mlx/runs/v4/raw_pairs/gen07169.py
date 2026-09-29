from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isin([21, 34]))
    .select(
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("s.end_date"),
    )
)
