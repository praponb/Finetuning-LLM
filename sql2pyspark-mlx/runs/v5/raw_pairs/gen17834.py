from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_running_2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 0.85), 2).alias("duration_sec_adjusted"),
        F.substring("page", 1, 4).alias("page_prefix"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "duration_sec_adjusted",
        F.sum("duration_sec_adjusted").over(w_running).alias("duration_sec_adjusted_sum_30d"),
        F.last("duration_sec_adjusted", ignorenulls=True).over(w_running_2).alias("last_known_duration_sec_adjusted"),
    )
)

valid_web_events = (
    metrics
    .filter(F.col("duration_sec_adjusted").isNotNull())
    .select(
        "*",
        (F.col("last_known_duration_sec_adjusted") - F.col("duration_sec_adjusted_sum_30d")).alias("last_known_duration_sec_adjusted_minus_duration_sec_adjusted_sum_30d"),
        F.floor("duration_sec_adjusted").alias("duration_sec_adjusted_whole"),
    )
)

kept_keys = (
    valid_web_events
    .select("id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([4, 16, 25]))
        .select("id")
    )
)

kept_rows = (
    valid_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "duration_sec_adjusted_sum_30d",
        "last_known_duration_sec_adjusted",
        "last_known_duration_sec_adjusted_minus_duration_sec_adjusted_sum_30d",
    )
)
