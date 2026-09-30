from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id")
w2 = Window.partitionBy("event_type")

cleaned = (
    df_web_events
    .filter(F.col("event_time") < "2024-04-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

kept_keys = (
    cleaned
    .select("event_type", "session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("event_time") < "2024-12-01")
        .select("event_type", "session_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isNull())
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.duration_sec",
        "a.event_type_title",
        "a.resolved_page",
        F.col("d.device"),
        F.col("d.country"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.duration_sec",
        "a.event_type_title",
        "a.resolved_page",
        F.col("b.browser"),
    )
)

filtered = (
    enriched
    .filter(F.col("browser").isNotNull() & (F.col("duration_sec") <= 137))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.round((F.col("duration_sec") * 100), 2).alias("duration_sec_adjusted"),
    )
)

result = (
    filtered
    .select(
        "id",
        "session_id",
        F.min("duration_sec_adjusted").over(w).alias("event_type_min_duration_sec_adjusted"),
        F.sum("duration_sec_adjusted").over(w2).alias("event_type_sum_duration_sec_adjusted"),
    )
)
