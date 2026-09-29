from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("burst_id", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("event_time", "id")
w6 = Window.orderBy("prev_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-01").cast("date")) & (F.col("event_time") < "2024-03-19"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
    .withColumn("page_filled_2", F.coalesce("page", F.lit("OTHER")))
)

joined = (
    base
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device").isin(["desktop", "tablet"]))
    .select("src.*", F.col("d.browser"))
)

gaps = (
    joined
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "page_filled",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

cleaned = (
    sessionized
    .filter(F.col("event_time") >= F.lit("2023-08-26").cast("date"))
    .select(
        "id",
        "event_time",
        "page_filled",
        "burst_id",
        (F.col("burst_id") - F.col("duration_sec")).alias("burst_id_minus_duration_sec"),
    )
)

metrics = (
    cleaned
    .withColumn("burst_id_minus_duration_sec_avg_2_prior", F.avg("burst_id_minus_duration_sec").over(w_part))
    .withColumn("last_known_burst_id_minus_duration_sec", F.last("burst_id_minus_duration_sec", ignorenulls=True).over(w4))
    .withColumn("prev_burst_id", F.lag("burst_id", 2, 0).over(w_ordered))
)

scored = (
    metrics
    .select(
        "id",
        "event_time",
        "burst_id",
        "burst_id_minus_duration_sec",
        "last_known_burst_id_minus_duration_sec",
        "prev_burst_id",
        F.min("prev_burst_id").over(w6).alias("running_min_prev_burst_id"),
        F.count("prev_burst_id").over(w6).alias("running_count_prev_burst_id"),
        F.avg("prev_burst_id").over(w6).alias("running_avg_prev_burst_id"),
        F.max("prev_burst_id").over(w6).alias("running_max_prev_burst_id"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_time",
        "burst_id",
        "prev_burst_id",
        "running_min_prev_burst_id",
        "running_count_prev_burst_id",
        "running_avg_prev_burst_id",
        "running_max_prev_burst_id",
    )
)
