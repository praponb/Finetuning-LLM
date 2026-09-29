from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("event_time", "id")
w2 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.browser"), F.col("b.country"), F.col("b.device"))
)

valid = (
    enriched
    .filter(F.col("event_time") >= F.to_timestamp(F.lit("2023-04-22 09:00:00")))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "browser",
        "country",
        F.weekofyear("event_time").alias("event_time_weekofyear"),
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

with_prev = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

filtered = (
    sessionized
    .filter(F.col("event_time_weekofyear") >= 73)
    .select(
        "id",
        "event_type",
        "event_time",
        "browser",
        "country",
        "event_time_weekofyear",
        "duration_sec_rounded",
        "resolved_country",
        "is_new_session",
        "island_id",
        F.greatest("island_id", "duration_sec_rounded").alias("greatest_island_id_duration_sec_rounded"),
        F.hour("prev_event_time").alias("prev_event_time_hour"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "event_time",
        "country",
        "event_time_weekofyear",
        "island_id",
        "greatest_island_id_duration_sec_rounded",
        "prev_event_time_hour",
        "country_clean",
    )
)
