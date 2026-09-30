from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page") == "/product")
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 501, "High").when(F.col("duration_sec") >= 205, "Medium").otherwise("Low"))
    .withColumn("event_type_upper", F.upper("event_type"))
)

with_dims = (
    base
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.device"))
)

cleaned = (
    with_dims
    .filter(F.col("event_time") > F.lit("2023-11-15").cast("date"))
    .withColumn("event_type_device_key", F.concat_ws("/", "event_type", "device"))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

enriched = (
    sessions_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.browser"), F.col("p.country"))
)

grouped = (
    enriched
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 513, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["session_no_tier", "duration_sec_tier", "page"], ["session_no_tier", "duration_sec_tier"], ["session_no_tier"], []],
        "session_no_tier",
        "duration_sec_tier",
        "page",
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.max("event_time").alias("last_seen"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min_2"),
    )
)

result = grouped.select("session_no_tier", "duration_sec_tier", "duration_min_2")
