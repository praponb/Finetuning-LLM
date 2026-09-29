from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.orderBy(F.col("duration_sec").desc())

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-02").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
    )
)

matched_keys = (
    filtered
    .select("id", "event_type")
    .intersect(
        df_web_events
        .filter(F.col("event_time") < "2025-09-01")
        .select("id", "event_type")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "id",
        "event_type",
        F.min("duration_sec").over(w).alias("event_type_min_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)

scored = (
    with_windows
    .filter(F.col("duration_sec_cume_dist") < 0.74)
    .select(
        "id",
        "event_type",
        "event_type_min_duration_sec",
        F.count("duration_sec_cume_dist").over(w).alias("event_type_count_duration_sec_cume_dist"),
    )
)

base = (
    scored
    .filter(F.col("event_type_count_duration_sec_cume_dist") >= 12)
    .select(
        "*",
        F.when(F.col("event_type_min_duration_sec") >= 410, "High").when(F.col("event_type_min_duration_sec") >= 227, "Medium").otherwise("Low").alias("event_type_min_duration_sec_tier"),
    )
)

result = base.select("id", "event_type", "event_type_min_duration_sec")
