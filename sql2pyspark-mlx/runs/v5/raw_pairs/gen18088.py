from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/cart", "/home"]))
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.home_count") > 7)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("ss.home_count"),
        F.col("ss.max_duration_sec"),
    )
    .orderBy(F.col("home_count").desc(), "id")
    .limit(10)
)
