from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("island_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.avg("island_id").alias("avg_island_id"),
    )
    .orderBy("island_id_tier")
)
