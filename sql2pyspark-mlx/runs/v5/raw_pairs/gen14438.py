from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isin([17, 20, 23]) & (F.to_date(F.col("s.started_at")) < "2024-09-02"))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.city"),
        F.col("c.name"),
        F.col("c.email"),
    )
)
