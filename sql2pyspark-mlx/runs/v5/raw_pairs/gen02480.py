from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .groupBy("session_no")
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.max(F.when(F.col("event_type") == "page_view", F.col("session_no"))).alias("page_view_max_session_no"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .orderBy("session_no")
)
