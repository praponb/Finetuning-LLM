from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy("duration_sec_max_30d", "id")
w5 = Window.orderBy(F.col("duration_sec_max_30d").desc_nulls_first())
w6 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("duration_sec_max_30d_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.orderBy("greatest_duration_sec_max_30d_session_id_count_duration_sec_rounded", "id")
w9 = Window.orderBy("session_id_count_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("prev_duration_sec_max_30d", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.orderBy("event_time", "id")
w12 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

enriched = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select(
        "id",
        "event_time",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.count("duration_sec").over(w2).alias("session_id_count_duration_sec"),
        F.max("duration_sec").over(w3).alias("duration_sec_max_30d"),
    )
)

filtered = (
    enriched
    .filter((F.col("event_time") >= F.lit("2023-05-25 22:00:00").cast("timestamp")) & (F.col("session_id_count_duration_sec") >= 8))
    .select(
        "id",
        "event_time",
        "session_id_count_duration_sec",
        "duration_sec_max_30d",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.round("session_id_count_duration_sec", 2).alias("session_id_count_duration_sec_rounded"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("duration_sec_max_30d", 2, 0).over(w4).alias("prev_duration_sec_max_30d"),
        F.cume_dist().over(w5).alias("duration_sec_max_30d_cume_dist"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("session_id_count_duration_sec") >= 9)
    .withColumn("greatest_duration_sec_max_30d_session_id_count_duration_sec_rounded", F.greatest("duration_sec_max_30d", "session_id_count_duration_sec_rounded"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.min("duration_sec_max_30d").over(w6).alias("running_min_duration_sec_max_30d"),
        F.avg("duration_sec_max_30d").over(w6).alias("running_avg_duration_sec_max_30d"),
        F.count("duration_sec_max_30d").over(w6).alias("running_count_duration_sec_max_30d"),
    )
)

scored = (
    metrics
    .withColumn("duration_sec_max_30d_cume_dist_avg_3", F.avg("duration_sec_max_30d_cume_dist").over(w7))
    .withColumn("next_running_count_duration_sec_max_30d", F.lead("running_count_duration_sec_max_30d", 1).over(w8))
    .withColumn("cumulative_prev_duration_sec_max_30d", F.sum("prev_duration_sec_max_30d").over(w9))
    .withColumn("session_id_count_duration_sec_rounded_avg_2", F.avg("session_id_count_duration_sec_rounded").over(w10))
)

with_windows_web_events = (
    scored
    .select(
        "id",
        "event_time",
        "session_id_count_duration_sec",
        "duration_sec_max_30d",
        "session_id_count_duration_sec_rounded",
        "duration_sec_max_30d_cume_dist",
        "running_min_duration_sec_max_30d",
        "duration_sec_max_30d_cume_dist_avg_3",
        "next_running_count_duration_sec_max_30d",
        "session_id_count_duration_sec_rounded_avg_2",
        F.row_number().over(w11).alias("event_seq"),
        F.sum("session_id_count_duration_sec_rounded_avg_2").over(w12).alias("session_id_count_duration_sec_rounded_avg_2_sum_7d"),
    )
)

result = (
    with_windows_web_events
    .groupBy(
        F.when(F.col("session_id_count_duration_sec_rounded_avg_2_sum_7d") >= 382, "LARGE").otherwise("SMALL").alias("session_id_count_duration_sec_rounded_avg_2_sum_7d_tier"),
        F.when(F.col("running_min_duration_sec_max_30d") >= 370, "HIGH").otherwise("LOW").alias("running_min_duration_sec_max_30d_tier"),
    )
    .agg(
        F.round(F.avg("duration_sec_max_30d_cume_dist"), 2).alias("avg_duration_sec_max_30d_cume_dist"),
        F.min("event_time").alias("first_seen"),
    )
)
