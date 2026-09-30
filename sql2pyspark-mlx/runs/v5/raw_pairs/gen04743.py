from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("country").orderBy(F.col("web_event_event_time").desc(), "id")

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

latest = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "browser", "country", "session_start_flag")
)

with_latest = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.session_start_flag",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

ranked = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.session_start_flag",
        "s.dedup_rank",
        F.col("c.event_time").alias("web_event_event_time_2"),
        F.col("c.event_type"),
    )
)

kept_keys = (
    with_last
    .select("id")
    .intersectAll(
        df_sessions
        .filter(F.col("started_at") < "2024-09-01")
        .select("id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.web_event_event_time_2")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "session_start_flag",
        "dedup_rank",
        "event_type",
        F.when(F.col("event_type").rlike("^(search|page_view)$"), "GROUP_B").when(F.col("event_type").rlike("^(click|add_to_cart)$"), "EXTENDED").otherwise("CORE").alias("event_type_zone"),
    )
    .orderBy("id")
)
