from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("event_time", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("duration_sec", "id")
w_part = Window.partitionBy("session_id")
w5 = Window.orderBy("first_known_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("session_id_stddev_samp_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("first_known_duration_sec", "id")
w9 = Window.orderBy("running_avg_last_known_session_id_stddev_samp_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-07-19 18:00:00"))) & (F.col("event_type") == "purchase"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

scored = (
    cleaned
    .filter(F.col("duration_sec") >= 248)
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "duration_sec_str",
        "page_filled",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6_prior"),
        F.first("duration_sec", ignorenulls=True).over(w2).alias("first_known_duration_sec"),
        F.ntile(3).over(w3).alias("duration_sec_bucket"),
        F.stddev_samp("duration_sec").over(w_part).alias("session_id_stddev_samp_duration_sec"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "duration_sec_str",
        "page_filled",
        "first_known_duration_sec",
        "duration_sec_bucket",
        "session_id_stddev_samp_duration_sec",
        F.last("session_id_stddev_samp_duration_sec", ignorenulls=True).over(w5).alias("last_known_session_id_stddev_samp_duration_sec"),
        F.sum("duration_sec_avg_6_prior").over(w6).alias("running_duration_sec_avg_6_prior"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "*",
        F.count("last_known_session_id_stddev_samp_duration_sec").over(w7).alias("running_count_last_known_session_id_stddev_samp_duration_sec"),
        F.avg("last_known_session_id_stddev_samp_duration_sec").over(w7).alias("running_avg_last_known_session_id_stddev_samp_duration_sec"),
        F.sum("last_known_session_id_stddev_samp_duration_sec").over(w7).alias("running_sum_last_known_session_id_stddev_samp_duration_sec"),
    )
)

filtered = (
    metrics_web_events
    .filter(F.col("running_sum_last_known_session_id_stddev_samp_duration_sec") <= 600)
    .select(
        "*",
        F.least("running_avg_last_known_session_id_stddev_samp_duration_sec", "running_sum_last_known_session_id_stddev_samp_duration_sec").alias("least_running_avg_last_known_session_id_stddev_samp_duration_sec_running_sum_last_known_session_id_stddev_samp_duration_sec"),
        F.coalesce("last_known_session_id_stddev_samp_duration_sec", F.lit(0)).alias("last_known_session_id_stddev_samp_duration_sec_filled"),
    )
)

scored_web_events = (
    filtered
    .select(
        "*",
        F.lag("least_running_avg_last_known_session_id_stddev_samp_duration_sec_running_sum_last_known_session_id_stddev_samp_duration_sec").over(w8).alias("prev_least_running_avg_last_known_session_id_stddev_samp_duration_sec_running_sum_last_known_session_id_stddev_samp_duration_sec"),
        F.lead("running_duration_sec_avg_6_prior").over(w8).alias("next_running_duration_sec_avg_6_prior"),
        F.lag("running_duration_sec_avg_6_prior").over(w8).alias("prev_running_duration_sec_avg_6_prior"),
        F.row_number().over(w8).alias("event_seq"),
        F.first("first_known_duration_sec", ignorenulls=True).over(w9).alias("first_known_first_known_duration_sec"),
    )
)

filtered_web_events = (
    scored_web_events
    .filter(F.col("duration_sec_bucket").between(3, 6))
    .select(
        "id",
        "duration_sec_str",
        "page_filled",
        "first_known_duration_sec",
        "duration_sec_bucket",
        "running_duration_sec_avg_6_prior",
        "running_avg_last_known_session_id_stddev_samp_duration_sec",
        "running_sum_last_known_session_id_stddev_samp_duration_sec",
        "last_known_session_id_stddev_samp_duration_sec_filled",
        "next_running_duration_sec_avg_6_prior",
        "prev_running_duration_sec_avg_6_prior",
        "first_known_first_known_duration_sec",
        F.when(F.col("running_sum_last_known_session_id_stddev_samp_duration_sec") >= 2280, "High").when(F.col("running_sum_last_known_session_id_stddev_samp_duration_sec") >= 310, "Medium").otherwise("Low").alias("running_sum_last_known_session_id_stddev_samp_duration_sec_level"),
    )
)

summary = (
    filtered_web_events
    .groupBy(
        F.when(F.col("running_avg_last_known_session_id_stddev_samp_duration_sec") >= 370, "LARGE").otherwise("SMALL").alias("running_avg_last_known_session_id_stddev_samp_duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("next_running_duration_sec_avg_6_prior"), 2).alias("avg_next_running_duration_sec_avg_6_prior"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select("running_avg_last_known_session_id_stddev_samp_duration_sec_tier")
    .orderBy("running_avg_last_known_session_id_stddev_samp_duration_sec_tier")
)
