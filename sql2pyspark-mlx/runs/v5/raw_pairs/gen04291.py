from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "session_id", "event_type", "page", "event_time", "duration_sec")
)

prev_events = deduped.withColumn("prev_event_time", F.lag("event_time").over(w2))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
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
        F.sum("new_group_flag").over(w3).alias("island_id"),
    )
)

remaining_keys = (
    sessions_web_events
    .select("session_id", "id")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id").isin([4, 6, 26, 28]))
        .select("session_id", "id")
    )
)

reconciled = (
    sessions_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "TH")
    .select("s.*", F.col("p.country"), F.col("p.device"), F.col("p.browser"))
)

result = (
    joined
    .withColumn("week_start", F.date_trunc("week", "event_time"))
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "island_id_tier"], ["week_start"]],
        "week_start",
        "island_id_tier",
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("row_count"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("island_id_per_click"),
    )
    .orderBy(F.col("row_count").desc())
)
