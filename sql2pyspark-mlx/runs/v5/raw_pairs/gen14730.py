from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .groupBy("event_type")
    .agg(
        F.avg(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec"))).alias("add_to_cart_avg_duration_sec"),
        F.sum("island_id").alias("total_island_id"),
    )
)
