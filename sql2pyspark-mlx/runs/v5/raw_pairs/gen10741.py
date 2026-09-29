from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w_ordered = Window.partitionBy("page")
w6 = Window.orderBy("page_sum_duration_sec_avg_6_prior", "id").rowsBetween(-6, -1)
w7 = Window.orderBy(F.col("duration_sec_avg_6_prior").desc(), "id")
w8 = Window.orderBy("duration_sec_percent_rank", "id")
w9 = Window.orderBy("duration_sec_avg_6_prior", "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-06 20:00:00").cast("timestamp")) & F.col("session_id").isin([10, 14, 15]))
    .withColumn("page_upper", F.upper("page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("DIRECT")))
)

base = (
    filtered
    .filter((F.col("event_time") >= F.lit("2023-08-09").cast("date")) & (F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_clean",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "page_clean",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6_prior"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "event_time",
        "page_clean",
        "duration_sec_avg_6_prior",
        F.avg("duration_sec_avg_6_prior").over(w3).alias("duration_sec_avg_6_prior_avg_3"),
        F.percent_rank().over(w4).alias("duration_sec_percent_rank"),
        F.sum("duration_sec_avg_6_prior").over(w_ordered).alias("page_sum_duration_sec_avg_6_prior"),
    )
)

with_windows_web_events = (
    scored
    .withColumn("duration_sec_avg_6_prior_avg_3_avg_6_prior", F.avg("duration_sec_avg_6_prior_avg_3").over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
    .withColumn("duration_sec_percent_rank_bucket", F.ntile(5).over(w8))
    .withColumn("prev_page_sum_duration_sec_avg_6_prior", F.lag("page_sum_duration_sec_avg_6_prior", 2, 0).over(w9))
)

filtered_web_events = (
    with_windows_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-17 01:00:00").cast("timestamp"))
    .select(
        "id",
        "event_time",
        "duration_sec_avg_6_prior_avg_3",
        "page_sum_duration_sec_avg_6_prior",
        "duration_sec_avg_6_prior_avg_3_avg_6_prior",
        "row_seq",
        "duration_sec_percent_rank_bucket",
        "prev_page_sum_duration_sec_avg_6_prior",
        F.least("duration_sec_avg_6_prior", "page_sum_duration_sec_avg_6_prior").alias("least_duration_sec_avg_6_prior_page_sum_duration_sec_avg_6_prior"),
        F.round((F.col("duration_sec_percent_rank") * 0.9), 2).alias("duration_sec_percent_rank_scaled"),
    )
)

result = (
    filtered_web_events
    .select(
        "id",
        "duration_sec_avg_6_prior_avg_3",
        "page_sum_duration_sec_avg_6_prior",
        "duration_sec_avg_6_prior_avg_3_avg_6_prior",
        "duration_sec_percent_rank_bucket",
        "prev_page_sum_duration_sec_avg_6_prior",
        "least_duration_sec_avg_6_prior_page_sum_duration_sec_avg_6_prior",
        "duration_sec_percent_rank_scaled",
    )
)
