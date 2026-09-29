from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    islands
    .cube(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("first_event_time"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("duration_sec_per_purchase"),
        F.max("event_time").alias("last_event_time"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
