from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "HIGH").otherwise("LOW").alias("new_group_flag_tier"),
        "session_id",
    )
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.min("event_time").alias("first_seen"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
    )
    .filter(F.col("search_events") >= 1)
)
