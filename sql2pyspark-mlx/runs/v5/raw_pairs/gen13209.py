from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc_nulls_last())
w2 = Window.partitionBy("browser", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device")
w4 = Window.partitionBy("country").orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("device").orderBy("event_time", "id")
w6 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("session_no", "id")
w8 = Window.partitionBy("device", "browser")
w_part_2 = Window.partitionBy("event_type").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .filter(F.col("d.country") == "SG")
    .select("s.*", F.col("d.device"), F.col("d.browser"), F.col("d.country"))
)

latest = (
    joined
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
        F.min("duration_sec").over(w3).alias("device_min_duration_sec"),
        F.ntile(3).over(w4).alias("duration_sec_bucket"),
    )
)

with_prev = metrics.withColumn("prev_event_time", F.lag("event_time").over(w5))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "device",
        "browser",
        "country",
        "rank_in_group",
        "device_min_duration_sec",
        "duration_sec_bucket",
        F.sum("new_group_flag").over(w6).alias("session_no"),
    )
)

with_windows = (
    sessions_web_events
    .select(
        "id",
        "device",
        "browser",
        "rank_in_group",
        "device_min_duration_sec",
        F.lead("duration_sec_bucket", 1, 0).over(w_part).alias("next_duration_sec_bucket"),
        F.round((F.col("duration_sec_bucket") / F.sum("duration_sec_bucket").over(w8)), 4).alias("duration_sec_bucket_share"),
        F.sum("session_no").over(w_part_2).alias("cumulative_session_no"),
    )
)

result = with_windows.select("id", "device", "browser", "rank_in_group")
