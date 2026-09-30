from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("browser", "device").orderBy(F.col("session_id").desc(), "id")
w6 = Window.partitionBy("device").orderBy("event_time", "id")
w7 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

with_latest = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.device", "s.browser", "s.session_id", F.col("c.event_time"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

ranked = (
    latest_match
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w7))

result = (
    islands
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["is_new_session_tier", "burst_id_tier", "burst_id"], ["is_new_session_tier", "burst_id_tier"], []],
        "is_new_session_tier",
        "burst_id_tier",
        "burst_id",
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.max("burst_id").alias("max_burst_id"),
        F.avg(F.when(F.col("device") == "desktop", F.col("is_new_session"))).alias("desktop_avg_is_new_session"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min_2"),
    )
)
