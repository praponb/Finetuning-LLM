from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w_hist = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w4 = Window.partitionBy("page").orderBy("prev_duration_sec", "id")
w_part = Window.partitionBy("session_id").orderBy("duration_sec_rank", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") == 162))
    .select(
        "*",
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

metrics = (
    cleaned
    .filter(F.col("duration_sec") != 346)
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.lag("duration_sec").over(w_running).alias("prev_duration_sec"),
        F.row_number().over(w_running).alias("row_seq"),
        F.sum("duration_sec").over(w_hist).alias("duration_sec_sum_7d"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.rank().over(w3).alias("duration_sec_rank"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

with_windows_web_events = (
    with_windows
    .withColumn("last4_sum_row_seq", F.sum("row_seq").over(w_part))
    .withColumn("last4_count_row_seq", F.count("row_seq").over(w_part))
    .withColumn("last4_min_row_seq", F.min("row_seq").over(w_part))
)

result = (
    with_windows_web_events
    .cube("session_id", F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.percentile_approx("last4_sum_row_seq", [0.25, 0.5, 0.75]).alias("last4_sum_row_seq_quartiles"),
        F.percentile_approx("duration_sec_rank", 0.9).alias("duration_sec_rank_p90"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec_rank"))).alias("search_total_duration_sec_rank"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sum(F.when(F.col("page") == "/search", F.col("row_seq"))).alias("search_total_row_seq"),
    )
    .orderBy(F.col("year_start").desc())
)
