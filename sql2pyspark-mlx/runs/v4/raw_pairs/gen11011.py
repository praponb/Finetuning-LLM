from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("num_web_events"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.num_web_events") >= 8)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("ss.num_web_events"),
    )
    .orderBy(F.col("id").desc())
)
