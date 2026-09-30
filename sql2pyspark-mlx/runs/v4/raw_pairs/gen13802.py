from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        "session_id",
        "event_type",
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.avg(F.when(F.col("event_type") == "page_view", F.col("island_id"))).alias("page_view_avg_island_id"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("span_minutes").desc())
)
