from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").asc_nulls_last(), "id")
w2 = Window.partitionBy("session_id", "device")
w3 = Window.partitionBy("device").orderBy(F.col("latest_rank_adjusted").asc_nulls_first())

latest = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter(F.col("event_time") >= F.to_timestamp(F.lit("2023-06-09 22:00:00")))
    .select(
        "*",
        F.round((F.col("latest_rank") * 1.1), 2).alias("latest_rank_adjusted"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

valid_web_events = (
    valid
    .filter(~F.col("page").eqNullSafe(F.lit("/cart")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "latest_rank",
        "latest_rank_adjusted",
        "resolved_page",
        F.col("event_time").cast("date").alias("event_time_day"),
        (F.col("duration_sec") - F.col("latest_rank")).alias("duration_sec_minus_latest_rank"),
    )
)

matched_keys = (
    valid_web_events
    .select("session_id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([4, 22, 27]))
        .select("session_id")
    )
)

verified = (
    valid_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/home", "/cart", "/checkout"]))
        .select("page")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isNull())
    .select(
        "src.session_id",
        "src.event_type",
        "src.latest_rank",
        "src.latest_rank_adjusted",
        "src.event_time_day",
        F.col("s.device"),
    )
)

scored = (
    enriched
    .select(
        "session_id",
        "event_type",
        "latest_rank",
        "device",
        F.max("latest_rank_adjusted").over(w2).alias("session_id_max_latest_rank_adjusted"),
        F.cume_dist().over(w3).alias("latest_rank_adjusted_cume_dist"),
    )
)

result = (
    scored
    .withColumn("latest_rank_adjusted_cume_dist_tier", F.when(F.col("latest_rank_adjusted_cume_dist") >= 0.86, "PREMIUM").otherwise("STANDARD"))
    .rollup("device", "latest_rank_adjusted_cume_dist_tier", "session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max(F.when(F.col("device") == "desktop", F.col("latest_rank_adjusted_cume_dist"))).alias("desktop_max_latest_rank_adjusted_cume_dist"),
        F.round(F.avg("session_id_max_latest_rank_adjusted"), 2).alias("avg_session_id_max_latest_rank_adjusted"),
        F.min("latest_rank_adjusted_cume_dist").alias("min_latest_rank_adjusted_cume_dist"),
        F.grouping("device").alias("grouping_id"),
    )
    .filter(F.col("min_latest_rank_adjusted_cume_dist") > 0)
    .orderBy(F.col("desktop_max_latest_rank_adjusted_cume_dist").asc_nulls_last())
)
