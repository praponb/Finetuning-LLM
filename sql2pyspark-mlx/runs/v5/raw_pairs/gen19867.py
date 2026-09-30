from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("duration_sec", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("duration_sec")
w_running = Window.partitionBy("page").orderBy("duration_sec_avg_5_prior", "id")
w6 = Window.partitionBy("page").orderBy("running_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("page", "event_type").orderBy("duration_sec_percent_rank", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w9 = Window.partitionBy("event_type", "page")

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-08-30 11:00:00").cast("timestamp"))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
    )
)

with_windows = (
    filtered
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "event_type_prefix",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.avg("duration_sec").over(w_part).alias("duration_sec_avg_5_prior"),
        F.sum("duration_sec").over(w3).alias("running_duration_sec_2"),
        F.percent_rank().over(w4).alias("duration_sec_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "event_type",
        "duration_sec_avg_5_prior",
        "duration_sec_percent_rank",
        F.ntile(4).over(w_running).alias("duration_sec_avg_5_prior_bucket"),
        F.sum("duration_sec_percent_rank").over(w6).alias("running_duration_sec_percent_rank"),
        F.avg("duration_sec").over(w7).alias("duration_sec_avg_3_prior"),
        F.lead("running_duration_sec", 1).over(w8).alias("next_running_duration_sec"),
        F.min("running_duration_sec_2").over(w9).alias("event_type_min_running_duration_sec_2"),
    )
)

matched_keys = (
    enriched
    .select("event_type", "id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") > 252)
        .select("event_type", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_web_events = (
    kept_rows
    .filter(F.col("next_running_duration_sec") != 1131)
    .withColumn("duration_sec_avg_5_prior_bucket_times_duration_sec_percent_rank", (F.col("duration_sec_avg_5_prior_bucket") * F.col("duration_sec_percent_rank")))
)

grouped = (
    filtered_web_events
    .groupBy(
        F.when(F.col("event_type_min_running_duration_sec_2") >= 2341, "HIGH").otherwise("LOW").alias("event_type_min_running_duration_sec_2_tier"),
        "event_type",
    )
    .agg(
        F.percentile_approx("duration_sec_percent_rank", 0.9).alias("duration_sec_percent_rank_p90"),
        F.avg("duration_sec_percent_rank").alias("avg_duration_sec_percent_rank"),
        F.max(F.when(F.col("event_type") == "page_view", F.col("duration_sec_avg_5_prior_bucket"))).alias("page_view_max_duration_sec_avg_5_prior_bucket"),
    )
    .filter(F.col("page_view_max_duration_sec_avg_5_prior_bucket") < 1000000)
)

result = (
    grouped
    .select(
        "event_type_min_running_duration_sec_2_tier",
        "event_type",
        "avg_duration_sec_percent_rank",
        "page_view_max_duration_sec_avg_5_prior_bucket",
    )
    .orderBy(F.col("event_type").desc_nulls_first())
)
