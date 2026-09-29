from pyspark.sql import functions as F

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    kept
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .rollup("year_start", "country")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.min("started_at").alias("first_seen"),
        F.max("started_at").alias("ended_at"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .orderBy(F.col("desktop_count").desc_nulls_last())
)
