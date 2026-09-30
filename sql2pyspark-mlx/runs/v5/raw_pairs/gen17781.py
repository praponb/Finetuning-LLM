from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time_dayofweek", "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter(F.col("session_id") == 27)
    .withColumn("event_time_dayofweek", F.dayofweek("event_time"))
    .withColumn("event_type_upper", F.upper("event_type"))
)

enriched = (
    cleaned
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w))
    .withColumn("prev_event_time_dayofweek", F.lag("event_time_dayofweek").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.event_time_dayofweek",
        "s.event_type_upper",
        "s.prev_duration_sec",
        F.col("p2.browser"),
        F.col("p2.country"),
        F.col("p2.device"),
    )
)

valid = (
    with_dims
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time_dayofweek",
        "event_type_upper",
        "prev_duration_sec",
        "browser",
        "country",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

matched_keys = (
    valid
    .select("id", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([3, 32, 35, 39]))
        .select("id", "session_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_time_dayofweek",
        "event_type_upper",
        "browser",
        "country",
        "resolved_country",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("browser").desc())
)
