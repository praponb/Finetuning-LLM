from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.duration_sec") <= 379)
    .groupBy(F.col("s.device"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/home", F.col("we.duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") > 4)
    .orderBy("home_duration_sec", "device")
)
