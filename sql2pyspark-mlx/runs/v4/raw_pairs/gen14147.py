from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page").orderBy("latest_rank", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

latest = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w3).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_6"),
        F.ntile(4).over(w_running).alias("duration_sec_bucket_2"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.browser"), F.col("s.device"), F.col("s.country"))
)

summary = (
    enriched
    .groupBy(
        "page",
        F.when(F.col("duration_sec_bucket_2") >= 7, "LARGE").otherwise("SMALL").alias("duration_sec_bucket_2_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("device") == "tablet", F.col("duration_sec")).otherwise(0)).alias("tablet_duration_sec"),
        F.round(F.sum("duration_sec_avg_6") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_avg_6_per_page_view"),
    )
    .filter(F.col("duration_sec_avg_6_per_page_view") >= 0)
)

result = (
    summary
    .select(
        "page",
        "duration_sec_bucket_2_tier",
        "week_start",
        "duration_sec_avg_6_per_page_view",
    )
    .orderBy(
        F.col("week_start").asc_nulls_first(),
        F.col("duration_sec_bucket_2_tier").desc(),
    )
)
