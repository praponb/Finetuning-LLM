from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-12-19 12:00:00"))) & F.col("page").isNotNull() & (F.col("event_type") != "page_view"))
    .withColumn("event_time_period", F.date_trunc("WEEK", "event_time"))
)

result = (
    base
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.browser"), F.col("p.country"), F.col("p.device"))
)
