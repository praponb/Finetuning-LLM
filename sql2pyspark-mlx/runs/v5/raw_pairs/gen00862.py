from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

remaining_keys = (
    df_web_events
    .select("id", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "purchase")
        .select("id", "session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/product", "/home", "/search"]))
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("event_type_page", F.concat("event_type", F.lit(" - "), "page"))
)

kept_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_web_events
        .filter(F.col("event_type").isin(["click", "page_view"]))
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.event_type_page",
        F.col("p.browser"),
    )
)

valid = (
    joined
    .filter(F.col("event_time") < "2023-04-01")
    .withColumn("event_time_weekofyear", F.weekofyear("event_time"))
)

result = (
    valid
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select("id", "session_id", "event_time", F.col("sum_web_event_duration_sec"))
    .orderBy(F.col("id").desc_nulls_last())
)
