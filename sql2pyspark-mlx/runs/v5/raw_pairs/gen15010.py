from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy(F.col("event_time").asc_nulls_first(), "id")
w3 = Window.partitionBy("device").orderBy("rn", "id")
w4 = Window.partitionBy("device").orderBy(F.col("event_time").desc(), "id")
w_recent = Window.partitionBy("device").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device").orderBy("event_time", "id")
w7 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("browser").isin(["Safari", "Chrome", "Firefox"]))
    .select("id", "device", "browser", F.length("device").alias("device_length"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.device", F.col("c.event_time"), F.col("c.duration_sec"))
)

deduped = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") != "add_to_cart"), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.lead("duration_sec", 2, 0).over(w3).alias("next_duration_sec"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("duration_sec").over(w_recent).alias("cumulative_duration_sec"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device",
        "event_time",
        "duration_sec",
        "next_duration_sec",
        "seq_no",
        F.sum("is_new_burst").over(w7).alias("session_id"),
    )
)

gaps_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_sessions = (
    gaps_2
    .select(
        "id",
        "device",
        "event_time",
        "duration_sec",
        "seq_no",
        "session_id",
        F.sum("new_group_flag").over(w7).alias("burst_id"),
    )
)

report = (
    islands_sessions
    .select(
        "id",
        "device",
        "event_time",
        "duration_sec",
        "seq_no",
        "session_id",
        "burst_id",
    )
    .orderBy(F.col("id").desc(), F.col("duration_sec").desc())
)

result = report.select("id", "event_time", "duration_sec", "seq_no", "session_id")
