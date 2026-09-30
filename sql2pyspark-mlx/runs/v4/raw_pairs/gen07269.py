from pyspark.sql import functions as F

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.max("started_at").alias("last_seen"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.max("started_at").alias("ended_at"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
    .orderBy("th_events")
)
