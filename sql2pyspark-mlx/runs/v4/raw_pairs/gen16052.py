from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("c.email"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
)
