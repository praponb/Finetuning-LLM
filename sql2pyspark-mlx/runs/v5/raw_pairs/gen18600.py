from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.avg("duration_sec").over(w_hist).alias("running_avg_duration_sec"),
        F.count("duration_sec").over(w_hist).alias("running_count_duration_sec"),
        F.sum("duration_sec").over(w_hist).alias("running_sum_duration_sec"),
        F.min("duration_sec").over(w_hist).alias("running_min_duration_sec"),
    )
)

filtered = (
    enriched
    .filter((F.col("event_time") > F.lit("2023-05-13").cast("date")) & F.col("session_id").isin([18, 31, 34, 35]))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "running_avg_duration_sec",
        "running_sum_duration_sec",
        "running_min_duration_sec",
        (F.col("running_count_duration_sec") * F.col("duration_sec")).alias("running_count_duration_sec_times_duration_sec"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

filtered_web_events = (
    sessionized
    .filter(F.col("event_time") >= F.lit("2023-05-16 06:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "running_avg_duration_sec",
        "running_sum_duration_sec",
        "running_count_duration_sec_times_duration_sec",
        "is_new_burst",
        "burst_id",
        F.to_date("event_time").alias("event_time_date"),
    )
)

result = (
    filtered_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.browser").alias("browser"),
        F.col("e.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.running_avg_duration_sec")).alias("max_running_avg_duration_sec"),
        F.round(F.sum(F.col("e.running_count_duration_sec_times_duration_sec")), 2).alias("total_running_count_duration_sec_times_duration_sec"),
    )
)
