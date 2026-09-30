from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.duration_sec_range") >= 27)
    .select(F.col("s.device"), F.col("st.total_count"), F.col("st.duration_sec_range"))
    .orderBy(F.col("duration_sec_range").desc(), "device")
)
