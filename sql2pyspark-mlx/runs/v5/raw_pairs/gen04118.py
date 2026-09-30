from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("c.city"),
        F.col("c.name"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.length(F.col("c.country")).alias("country_length"),
    )
)
