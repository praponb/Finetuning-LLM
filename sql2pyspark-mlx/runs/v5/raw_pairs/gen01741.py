from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page") == "/checkout"), "left_anti")
)

remaining_keys = (
    eligible
    .select("id", "browser")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["TH", "DE", "SG"]))
        .select("id", "browser")
    )
)

verified = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.web_event_event_time")), "left_semi")
)

enriched = (
    not_flagged
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.age"), F.col("c.email"))
)

grouped = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "web_event_event_time").alias("quarter_start"),
        F.when(F.col("age") >= 56, "HIGH").otherwise("LOW").alias("age_tier"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.sum(F.when(F.col("browser") == "Safari", F.col("age")).otherwise(0)).alias("safari_age"),
        F.round(F.avg("age"), 2).alias("avg_age_2"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
)

result = (
    grouped
    .select(
        "quarter_start",
        "age_tier",
        "week_start",
        "tablet_events",
        "avg_age",
        "avg_age_2",
        "duration_min",
    )
    .orderBy(F.col("age_tier").desc(), "avg_age_2")
)
