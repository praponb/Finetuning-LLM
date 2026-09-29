from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-03-27 22:00:00").cast("timestamp")) & (~F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.upper("page").alias("page_upper"),
        F.initcap(F.trim("page")).alias("page_title"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .rollup("session_id", "page")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.grouping("session_id").alias("gid"),
    )
)
