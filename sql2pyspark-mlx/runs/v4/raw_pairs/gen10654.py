from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec").between(324, 466) & F.col("s.country").isNotNull())
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.coalesce(F.col("we.page"), F.lit("Unknown")).alias("page_filled"),
    )
    .orderBy(F.col("session_id").desc())
)
