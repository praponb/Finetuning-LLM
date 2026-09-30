from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(~F.col("page").isin(["/search", "/product", "/checkout"]))
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.duration_sec_range") >= 55)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("st.duration_sec_range"),
        F.col("st.stddev_duration_sec"),
    )
    .orderBy("duration_sec_range")
)
