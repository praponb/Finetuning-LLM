from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "session_id").orderBy(F.col("duration_sec").desc_nulls_first())
w4 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device", "browser").orderBy(F.col("duration_sec").asc_nulls_first(), "id")

base = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/cart")))
    .select(
        "*",
        F.round((F.col("duration_sec") * 1.1), 2).alias("duration_sec_adjusted"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_adjusted",
        "page_clean",
        F.sum("is_new_burst").over(w2).alias("session_id_2"),
    )
)

enriched = (
    sessions_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "DE")
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.duration_sec",
        "e.page_clean",
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.country"),
    )
)

enriched_web_events = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "browser",
        "device",
        F.cume_dist().over(w3).alias("duration_sec_cume_dist"),
        F.last("device", ignorenulls=True).over(w4).alias("last_known_device"),
    )
)

latest = (
    enriched_web_events
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "session_id",
        "duration_sec",
        "browser",
        "device",
        "duration_sec_cume_dist",
        "last_known_device",
    )
)

with_dims = (
    latest
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.duration_sec",
        "e.browser",
        "e.last_known_device",
        F.col("b.browser").alias("session_browser"),
        F.col("b.country"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("duration_sec") >= 371, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "browser",
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
    .filter(F.col("duration_sec_stddev") < 1000000)
)
