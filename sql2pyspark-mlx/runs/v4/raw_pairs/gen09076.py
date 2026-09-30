from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.hour("event_time").between(8, 12))
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("S.duration_sec_range"),
        F.col("S.event_type_list"),
    )
    .orderBy("customer_id", F.col("duration_sec_range").desc())
)
