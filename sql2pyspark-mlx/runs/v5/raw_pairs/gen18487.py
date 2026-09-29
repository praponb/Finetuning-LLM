from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "country").orderBy(F.col("session_id").desc())
w_grp = Window.partitionBy("device")
w5 = Window.partitionBy("browser").orderBy(F.col("session_id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("browser").orderBy("event_time", "id")
w9 = Window.partitionBy("browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("browser").orderBy("prev_event_time", "id")
w11 = Window.partitionBy("browser").orderBy("prev_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .withColumn("device_title", F.initcap(F.trim("device")))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
)

with_prev = cleaned.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

scored = (
    sessionized
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "browser_filled",
        "is_new_burst",
        F.cume_dist().over(w3).alias("session_id_cume_dist"),
        F.min("is_new_burst").over(w_grp).alias("device_min_is_new_burst"),
        F.dense_rank().over(w5).alias("session_id_dense_rank"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.country",
        "s.browser_filled",
        "s.is_new_burst",
        "s.session_id_dense_rank",
        F.col("c.event_type"),
        F.col("c.event_time"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.event_time")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.event_time",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

with_prev_2 = latest_match.withColumn("prev_event_time", F.lag("event_time").over(w8))

with_gaps = (
    with_prev_2
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w9))

flagged_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("prev_event_time") - F.lag("prev_event_time").over(w10) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized_sessions = (
    flagged_2
    .select(
        "id",
        "browser",
        "event_time",
        "web_event_event_time",
        "prev_event_time",
        F.sum("new_group_flag").over(w11).alias("island_id"),
    )
)

result = (
    sessionized_sessions
    .select("id", "prev_event_time")
    .orderBy(F.col("prev_event_time").desc())
)
