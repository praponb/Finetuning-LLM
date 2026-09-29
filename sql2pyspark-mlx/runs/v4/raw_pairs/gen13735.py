from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").asc_nulls_last())

base = (
    df_web_events
    .filter(F.col("page") != "/home")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.concat("page", F.lit("/"), "event_type").alias("page_event_type"),
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.device"), F.col("b.browser"), F.col("b.country"))
)

latest = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("MONTH", "event_time_day").alias("activity_month"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
