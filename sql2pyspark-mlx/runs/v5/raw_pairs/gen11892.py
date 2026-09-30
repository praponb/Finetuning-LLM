from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser", "device").orderBy(F.col("web_event_duration_sec").desc_nulls_last())
w3 = Window.partitionBy("event_type").orderBy("days_since_started_at", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("web_event_page").orderBy("dedup_rank", "id")
w5 = Window.partitionBy("web_event_page").orderBy("started_at", "id")
w6 = Window.partitionBy("web_event_page").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.country",
        F.col("c.event_type"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

cleaned = (
    latest_match
    .filter((F.col("started_at") > F.lit("2023-04-14").cast("date")) & F.col("web_event_page").isNotNull())
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_semi")
)

latest = (
    kept
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("event_type") == "add_to_cart")
    .select(
        "*",
        F.avg("days_since_started_at").over(w3).alias("days_since_started_at_avg_5"),
        F.lead("web_event_duration_sec", 2).over(w4).alias("next_web_event_duration_sec"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "event_type",
        "web_event_duration_sec",
        "web_event_page",
        "web_event_event_time",
        "days_since_started_at",
        "dedup_rank",
        "days_since_started_at_avg_5",
        "next_web_event_duration_sec",
        F.sum("is_new_session").over(w6).alias("session_no"),
    )
)

result = (
    sessions_sessions
    .withColumn("days_since_started_at_tier", F.when(F.col("days_since_started_at") >= 76, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["session_no", "days_since_started_at_tier"], ["session_no"], []],
        "session_no",
        "days_since_started_at_tier",
    )
    .agg(
        F.round(F.avg("days_since_started_at"), 2).alias("avg_days_since_started_at"),
        F.min("web_event_event_time").alias("first_seen"),
        F.percentile_approx("days_since_started_at", 0.9).alias("days_since_started_at_p90"),
    )
    .filter(F.col("days_since_started_at_p90") >= 0)
)
