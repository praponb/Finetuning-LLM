from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("we.page").isin(["/checkout", "/search"]) & F.col("c.segment").isin(["Corporate", "SMB", "Consumer"]))
    .select(
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("c.segment"),
    )
    .orderBy(F.col("customer_id").desc())
)
