from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("duration_sec") >= 496, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)
