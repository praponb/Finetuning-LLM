from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w_recent = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.browser"), F.col("s.device"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.max("prev_duration_sec").over(w_recent).alias("running_max_prev_duration_sec"),
        F.min("prev_duration_sec").over(w_recent).alias("running_min_prev_duration_sec"),
        F.count("prev_duration_sec").over(w_recent).alias("running_count_prev_duration_sec"),
        F.sum("prev_duration_sec").over(w_recent).alias("running_sum_prev_duration_sec"),
    )
)

enriched_web_events = (
    enriched
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.duration_sec",
        "e.browser",
        "e.running_duration_sec",
        "e.running_min_prev_duration_sec",
        "e.running_sum_prev_duration_sec",
        F.col("d.device").alias("session_device"),
    )
)

result = (
    enriched_web_events
    .groupBy("browser")
    .pivot("session_device", ["mobile", "tablet"])
    .agg(
        F.sum("running_duration_sec").alias("total"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
