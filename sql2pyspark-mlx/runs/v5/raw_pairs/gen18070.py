from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["Consumer", "Corporate", "SMB"]))
    .select(
        F.col("c.name"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
)
