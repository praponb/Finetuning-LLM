from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("span_minutes").desc())
)
