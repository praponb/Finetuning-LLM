from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.device").isin(["desktop", "tablet", "mobile"]))
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
        F.sum(F.when(F.col("we.page") == "/home", F.col("we.duration_sec")).otherwise(0)).alias("home_duration_sec"),
    )
    .orderBy(F.col("home_duration_sec").desc(), F.col("id").desc(), "browser")
    .limit(1)
)
