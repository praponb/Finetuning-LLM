from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~F.col("s.country").isin(["JP", "DE", "US"]) | (F.col("we.event_type") == "purchase"))
    .select(
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("c.name"),
    )
)
