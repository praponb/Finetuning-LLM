from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("duration_sec").asc_nulls_last())
w2 = Window.partitionBy("country").orderBy("event_time", "id")
w3 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("browser").orderBy("latest_rank", "id")
w5 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)

enriched = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.browser"), F.col("d.country"))
)

latest = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_gaps = (
    latest
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "browser",
        "country",
        "latest_rank",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

scored = (
    sessions_web_events
    .filter(F.col("latest_rank") >= 1)
    .select(
        "*",
        F.lag("duration_sec", 1).over(w_hist).alias("prev_duration_sec"),
        F.avg("duration_sec").over(w5).alias("duration_sec_avg_2"),
    )
)

with_gaps_2 = (
    scored
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events_2 = with_gaps_2.withColumn("session_id", F.sum("session_start_flag").over(w3))

result = (
    sessions_web_events_2
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("burst_id")).otherwise(0)).alias("click_burst_id"),
        F.round(F.avg("duration_sec_avg_2"), 2).alias("avg_duration_sec_avg_2"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.sum("session_id").alias("total_session_id"),
    )
    .filter(F.col("avg_duration_sec_avg_2") > 0)
    .orderBy(F.col("year_start").asc_nulls_last())
)
