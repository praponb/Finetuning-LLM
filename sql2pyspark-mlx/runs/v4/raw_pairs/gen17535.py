from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .cube(
        F.date_trunc("month", "event_time").alias("activity_month"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
        F.date_trunc("week", "event_time").alias("week_start"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("is_new_session").alias("total_is_new_session"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.max("event_time").alias("ended_at"),
        F.count("*").alias("row_count"),
    )
)
