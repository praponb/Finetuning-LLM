from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_last())
w2 = Window.partitionBy("session_id", "event_type").orderBy("event_time_hour", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("session_device").orderBy(F.col("event_time").desc_nulls_last(), "id")
w4 = Window.partitionBy("country").orderBy("event_time", "id")
w5 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-04 12:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/product"))))
    .select("*", F.col("event_time").cast("date").alias("event_time_day"))
)

metrics = (
    valid
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "page_lower",
        "event_time_hour",
        "page_filled",
        "event_time_day",
        F.cume_dist().over(w).alias("duration_sec_cume_dist"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6_prior"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.browser"), F.col("d.device"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id").isin([18, 19, 33]))
    .select(
        "s.*",
        F.col("p.browser").alias("session_browser"),
        F.col("p.device").alias("session_device"),
        F.col("p.country"),
    )
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

gaps = (
    deduped
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("is_new_session").over(w5))

result = (
    sessions_web_events
    .select(
        "id",
        "page",
        "event_time",
        "event_time_hour",
        "page_filled",
        "event_time_day",
        "duration_sec_cume_dist",
        "duration_sec_avg_6_prior",
        "browser",
    )
    .orderBy(F.col("browser").asc_nulls_last())
)
