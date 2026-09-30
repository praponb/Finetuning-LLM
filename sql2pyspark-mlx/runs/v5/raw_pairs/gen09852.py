from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser")
w_running = Window.orderBy("duration_sec", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("country", "browser").orderBy(F.col("duration_sec_avg_3_prior").desc(), "id")

enriched = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.event_time",
        "a.duration_sec",
        F.col("s.browser"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.duration_sec",
        "s.browser",
        F.col("d.country"),
        F.col("d.browser").alias("session_browser"),
        F.col("d.device"),
    )
)

enriched_web_events = (
    with_dims
    .filter(F.col("session_browser").isin(["Chrome", "Firefox"]))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w_recent))
    .withColumn("browser_sum_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("duration_sec_avg_3_prior", F.avg("duration_sec").over(w_running))
)

latest = (
    enriched_web_events
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    latest
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.duration_sec",
        "src.device",
        "src.running_duration_sec",
        "src.duration_sec_avg_3_prior",
        "src.dedup_rank",
        F.col("s.country").alias("session_country"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("duration_sec") >= 422, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec_avg_3_prior", 0.9).alias("duration_sec_avg_3_prior_p90"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.count(F.when(F.col("session_country") == "JP", 1)).alias("jp_count"),
        F.round(F.avg("running_duration_sec"), 2).alias("avg_running_duration_sec"),
        F.size(F.collect_set("session_country")).alias("num_session_countries"),
    )
)
