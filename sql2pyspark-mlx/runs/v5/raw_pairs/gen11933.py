from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.start_date"),
        F.col("c.name"),
        F.col("c.age"),
    )
)
