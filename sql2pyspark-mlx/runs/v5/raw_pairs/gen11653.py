from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("event_type") != "click")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.to_date("event_time").alias("event_time_date"),
    )
)

prev_events = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "event_time_date",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("island_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("island_id_per_search"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.max("event_time").alias("last_seen"),
    )
    .orderBy(F.col("island_id_tier").desc_nulls_first())
)
