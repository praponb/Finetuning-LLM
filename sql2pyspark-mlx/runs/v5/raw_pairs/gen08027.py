from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-07-31 12:00:00").cast("timestamp"))
    .withColumn("event_time_month", F.month("event_time"))
)

kept_keys = (
    cleaned
    .select("page", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/cart", "/home"]))
        .select("page", "event_type")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.browser") == "Safari")
    .select("s.*", F.col("p.country"), F.col("p.device"))
)

joined_web_events = (
    joined
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        "s.device",
        F.col("p.device").alias("session_device"),
        F.col("p.country").alias("session_country"),
    )
)

filtered = (
    joined_web_events
    .filter((F.col("event_time") > F.lit("2023-05-11 23:00:00").cast("timestamp")) & F.col("duration_sec").between(144, 250))
    .select(
        "*",
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

joined_web_events_2 = (
    filtered
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select(
        "s.*",
        F.col("b.country"),
        F.col("b.browser"),
        F.col("b.device").alias("session_device_2"),
    )
)

result = (
    joined_web_events_2
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "session_device",
        "session_country",
        "is_checkout",
        "page_filled",
        "country",
        F.col("sum_web_event_duration_sec"),
    )
)
