from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy("new_group_flag", "id").rowsBetween(-2, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

with_gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w4))

joined = (
    islands
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.device") == "tablet")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        "s.is_new_burst",
        "s.session_id_2",
        "s.new_group_flag",
        F.col("p.country"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "is_new_burst",
        "new_group_flag",
        F.count("is_new_burst").over(w_grp).alias("last3_count_is_new_burst"),
        F.min("is_new_burst").over(w_grp).alias("last3_min_is_new_burst"),
        F.max("is_new_burst").over(w_grp).alias("last3_max_is_new_burst"),
    )
)

result = (
    metrics
    .select("id", "last3_count_is_new_burst", "last3_min_is_new_burst")
    .orderBy("last3_count_is_new_burst", F.col("id").desc())
)
