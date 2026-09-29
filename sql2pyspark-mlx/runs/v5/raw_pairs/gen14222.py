from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.max("started_at").alias("ended_at"),
        F.min("started_at").alias("first_seen"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
