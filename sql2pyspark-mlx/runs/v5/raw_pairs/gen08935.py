from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.to_date(F.col("s.started_at")) >= "2024-03-01") & (F.col("we.duration_sec") > 157))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("c.signup_date"),
        F.col("c.country"),
    )
    .orderBy(F.col("id").desc())
)
