from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_time") < "2025-09-13")
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("st.search_count"))
)
