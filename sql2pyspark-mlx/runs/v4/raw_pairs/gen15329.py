from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec").between(123, 445))
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("t.avg_duration_sec"),
        F.col("t.checkout_count"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
