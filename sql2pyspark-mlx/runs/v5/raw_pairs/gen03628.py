from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_web_events
    .withColumn("week_start", F.date_trunc("week", "event_time"))
    .cube("week_start")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.min("session_no").alias("min_session_no"),
        F.grouping("week_start").alias("grouping_id"),
    )
)
