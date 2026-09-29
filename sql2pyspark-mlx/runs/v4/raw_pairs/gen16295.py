from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.started_at") < "2024-12-07")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
        F.coalesce(F.col("s.browser"), F.lit("Unknown")).alias("browser_filled"),
    )
)
