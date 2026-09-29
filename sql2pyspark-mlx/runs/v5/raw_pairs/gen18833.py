from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") >= "2023-05-13")
    .withColumn("event_time_month", F.month("event_time"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

with_prev = filtered.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("island_id")
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)
