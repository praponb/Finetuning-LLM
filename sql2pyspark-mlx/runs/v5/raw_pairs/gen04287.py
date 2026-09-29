from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("session_id_2", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("browser").orderBy("session_id_2", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("device", "browser").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("event_time").desc(), "id")
w7 = Window.partitionBy("device", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("browser").orderBy("session_id_2_avg_5_prior", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("browser").orderBy(F.col("duration_sec_avg_5_prior").desc(), "id")
w10 = Window.partitionBy("device").orderBy("last7_avg_duration_sec", "id")
w_part = Window.partitionBy("device").orderBy("duration_sec", "id")
w12 = Window.partitionBy("device").orderBy(F.col("running_session_id_2").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_id_2"),
    )
)

with_dims = (
    islands
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_time",
        "e.duration_sec",
        "e.session_id_2",
        F.col("d.browser"),
        F.col("d.device"),
    )
)

metrics = (
    with_dims
    .filter(F.col("device") == "mobile")
    .withColumn("session_id_2_avg_5_prior", F.avg("session_id_2").over(w3))
    .withColumn("duration_sec_avg_5_prior", F.avg("duration_sec").over(w4))
    .withColumn("first_known_device", F.first("device", ignorenulls=True).over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("running_session_id_2", F.sum("session_id_2").over(w7))
)

scored = (
    metrics
    .select(
        "*",
        F.avg("duration_sec").over(w_running).alias("last7_avg_duration_sec"),
        F.sum("duration_sec").over(w_running).alias("last7_sum_duration_sec"),
        F.max("duration_sec").over(w_running).alias("last7_max_duration_sec"),
        F.ntile(10).over(w9).alias("duration_sec_avg_5_prior_bucket"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest
    .withColumn("prev_duration_sec_avg_5_prior", F.lag("duration_sec_avg_5_prior").over(w_part))
    .withColumn("prev_last7_avg_duration_sec", F.lag("last7_avg_duration_sec").over(w_part))
    .withColumn("next_last7_avg_duration_sec", F.lead("last7_avg_duration_sec").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("row_seq", F.row_number().over(w12))
)

with_dims_web_events = (
    enriched
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"), F.col("p.device").alias("session_device"))
)

result = (
    with_dims_web_events
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "session_id_2",
        "browser",
        "device",
        "session_id_2_avg_5_prior",
        "duration_sec_avg_5_prior",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "GROUP_A").when(F.col("device").rlike("^(tablet)$"), "GROUP_B").otherwise("EXTENDED").alias("device_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("device").desc(), F.col("duration_sec").desc())
)
