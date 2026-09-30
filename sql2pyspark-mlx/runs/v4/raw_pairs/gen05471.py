from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .rollup("year_start")
    .agg(
        F.avg("session_no").alias("avg_session_no"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count("*").alias("num_rows"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
