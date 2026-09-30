from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy("session_id_2", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("new_group_flag", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("device").orderBy("event_time", "id")
w8 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

flagged_2 = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_web_events = flagged_2.withColumn("session_id_2", F.sum("new_group_flag").over(w4))

metrics = (
    sessionized_web_events
    .filter(F.col("island_id") < 2)
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "session_id_2",
        F.max("session_id_2").over(w_grp).alias("last7_max_session_id_2"),
        F.avg("session_id_2").over(w_grp).alias("last7_avg_session_id_2"),
        F.count("session_id_2").over(w_grp).alias("last7_count_session_id_2"),
        F.avg("new_group_flag").over(w6).alias("new_group_flag_avg_3_prior"),
    )
)

gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_web_events_2 = gaps.withColumn("session_id_3", F.sum("session_start_flag").over(w2))

joined = (
    sessionized_web_events_2
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.country"), F.col("b.device"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_time",
        "src.last7_avg_session_id_2",
        "src.last7_count_session_id_2",
        "src.new_group_flag_avg_3_prior",
        "src.session_start_flag",
        "src.device",
        F.col("p.device").alias("session_device"),
        F.col("p.browser"),
    )
)

with_prev_2 = with_dims.withColumn("prev_event_time", F.lag("event_time").over(w7))

gaps_2 = (
    with_prev_2
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_web_events_3 = gaps_2.withColumn("burst_id", F.sum("is_new_burst").over(w8))

result = (
    sessionized_web_events_3
    .select(
        "id",
        "session_id",
        "page",
        "last7_avg_session_id_2",
        "last7_count_session_id_2",
        "new_group_flag_avg_3_prior",
        "session_start_flag",
        "device",
        "browser",
    )
    .orderBy(F.col("device").desc())
)
