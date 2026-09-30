from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.browser"), F.col("s.device"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
    )
    .filter(F.col("home_count") >= 33)
    .orderBy(F.col("browser").desc())
)
