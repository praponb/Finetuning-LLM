from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("device").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-27").cast("date")) & ~F.col("device").isin(["tablet", "mobile"]))
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.concat("country", F.lit(" "), "browser").alias("country_browser"),
    )
)

filtered = (
    cleaned
    .filter((F.col("device") == "mobile") & F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "country",
        "country_browser",
        F.substring("device", 1, 2).alias("device_prefix"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

scored = (
    latest_match
    .select(
        "id",
        "device",
        "country_browser",
        "country_filled",
        F.max("web_event_duration_sec").over(w_hist).alias("running_max_web_event_duration_sec"),
        F.min("web_event_duration_sec").over(w_hist).alias("running_min_web_event_duration_sec"),
        F.avg("web_event_duration_sec").over(w_hist).alias("running_avg_web_event_duration_sec"),
        F.sum("web_event_duration_sec").over(w_hist).alias("running_sum_web_event_duration_sec"),
    )
)

remaining_keys = (
    scored
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.col("started_at") < "2024-06-02")
        .select("device")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

report = (
    verified
    .select(
        "device",
        "country_browser",
        "country_filled",
        "running_max_web_event_duration_sec",
        "running_min_web_event_duration_sec",
        "running_avg_web_event_duration_sec",
        "running_sum_web_event_duration_sec",
    )
    .orderBy(F.col("running_sum_web_event_duration_sec").asc_nulls_last())
)

result = (
    report
    .select(
        "device",
        "country_browser",
        "running_min_web_event_duration_sec",
        "running_avg_web_event_duration_sec",
        "running_sum_web_event_duration_sec",
    )
)
