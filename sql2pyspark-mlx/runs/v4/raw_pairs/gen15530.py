from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .filter(F.col("S.page_view_count") >= 50)
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("S.home_count"),
        F.col("S.page_view_count"),
    )
    .orderBy("page_view_count", "browser")
)
