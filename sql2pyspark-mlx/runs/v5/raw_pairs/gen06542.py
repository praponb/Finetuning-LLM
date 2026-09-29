from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("event_time", "id")
w4 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("event_time", "id")
w6 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

joined = (
    islands
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.device").isin(["tablet", "desktop"]))
    .select(
        "e.id",
        "e.session_id",
        "e.event_time",
        "e.session_no",
        F.col("s.device"),
        F.col("s.country"),
    )
)

gaps = (
    joined
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_web_events = gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w4))

with_gaps_2 = (
    islands_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("session_no_2", F.sum("session_start_flag").over(w6))

result = (
    sessionized
    .alias("src")
    .join(df_sessions.alias("dim"), F.col("src.session_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.device").alias("session_device"),
        F.col("dim.country").alias("session_country"),
        F.col("src.device").alias("web_event_device"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("src.session_no")), 2).alias("total_session_no"),
        F.sum(F.col("src.session_no_2")).alias("total_session_no_2"),
    )
)
