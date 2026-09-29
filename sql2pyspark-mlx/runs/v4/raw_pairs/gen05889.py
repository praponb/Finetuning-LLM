from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time_day", "id")
w3 = Window.partitionBy("browser", "event_type").orderBy("duration_sec", "id")
w4 = Window.orderBy("event_time_day", "id")
w5 = Window.orderBy("event_time_quarter", "id")
w6 = Window.partitionBy("browser", "page").orderBy(F.col("duration_sec_rounded").desc_nulls_last())

base = (
    df_web_events
    .filter(F.col("event_type") != "click")
    .withColumn("event_time_day", F.col("event_time").cast("date"))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 0))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    deduped
    .filter(F.col("event_time") > F.lit("2023-07-24 21:00:00").cast("timestamp"))
    .withColumn("event_time_quarter", F.quarter("event_time"))
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("DIRECT")))
)

enriched = (
    filtered
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.browser"), F.col("p.country"), F.col("p.device"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.lag("event_time_quarter", 2).over(w4).alias("prev_event_time_quarter"),
        F.ntile(5).over(w5).alias("event_time_quarter_bucket"),
        F.cume_dist().over(w6).alias("duration_sec_rounded_cume_dist"),
    )
)

result = (
    metrics
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .rollup("activity_month")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.max("event_time").alias("last_seen"),
        F.grouping("activity_month").alias("grouping_id"),
    )
)
