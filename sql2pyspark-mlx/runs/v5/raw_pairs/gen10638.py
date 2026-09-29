from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.duration_sec_range") >= 61)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("t.duration_sec_range"),
    )
    .orderBy(F.col("id").desc())
)
