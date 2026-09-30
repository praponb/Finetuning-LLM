from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("page")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.to_timestamp(F.lit("2024-04-02 02:00:00")))
    .withColumn("page_length", F.length("page"))
)

remaining_keys = (
    cleaned
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(137, 455))
        .select("page", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.count("duration_sec").over(w2).alias("page_count_duration_sec"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"), F.col("p.device"))
)

base = (
    with_dims
    .filter((F.col("event_time") >= F.lit("2024-01-10").cast("date")) & F.col("device").isNotNull())
    .select(
        "*",
        F.when(F.col("duration_sec") >= 436, "High").when(F.col("duration_sec") >= 217, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
)

with_dims_web_events = (
    base
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.device").alias("session_device"), F.col("p.browser"))
)

result = (
    with_dims_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "page_length",
        "duration_sec_percent_rank",
        "device",
        "duration_sec_level",
        "session_device",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "CORE").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("event_time").desc_nulls_first())
)
