from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("page").orderBy(F.col("duration_sec_avg_5").asc_nulls_last(), "id")
w7 = Window.orderBy(F.col("duration_sec_avg_5").asc_nulls_first())
w8 = Window.partitionBy("page")
w9 = Window.partitionBy("page").orderBy(F.col("cumulative_duration_sec").asc_nulls_last())
w10 = Window.orderBy(F.col("cumulative_duration_sec_cume_dist").desc_nulls_first())
w_hist = Window.orderBy(F.col("duration_sec_bucket").asc_nulls_last())
w12 = Window.partitionBy("page").orderBy(F.col("prev_duration_sec").desc())

scored = (
    df_web_events
    .select(
        "id",
        "page",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.ntile(10).over(w3).alias("duration_sec_bucket"),
        F.last("duration_sec", ignorenulls=True).over(w4).alias("last_known_duration_sec"),
        F.avg("duration_sec").over(w5).alias("duration_sec_avg_5"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([1, 13, 35]))
        .select("page", "id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "page",
        "prev_duration_sec",
        "cumulative_duration_sec",
        "duration_sec_bucket",
        "duration_sec_avg_5",
        "rank_in_group",
        F.upper("page").alias("page_upper"),
        F.trim("page").alias("clean_page"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

metrics = (
    valid
    .filter(F.col("rank_in_group") >= 1)
    .select(
        "page",
        "prev_duration_sec",
        "duration_sec_bucket",
        "duration_sec_avg_5",
        "page_upper",
        "clean_page",
        "page_clean",
        F.dense_rank().over(w7).alias("duration_sec_avg_5_dense_rank"),
        F.stddev_samp("duration_sec_bucket").over(w8).alias("page_stddev_samp_duration_sec_bucket"),
        F.cume_dist().over(w9).alias("cumulative_duration_sec_cume_dist"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "prev_duration_sec",
        "duration_sec_avg_5",
        "clean_page",
        "duration_sec_avg_5_dense_rank",
        F.percent_rank().over(w10).alias("cumulative_duration_sec_cume_dist_percent_rank"),
        F.rank().over(w_hist).alias("duration_sec_bucket_rank"),
        F.cume_dist().over(w12).alias("prev_duration_sec_cume_dist"),
    )
)

result = (
    metrics_web_events
    .select(
        "prev_duration_sec",
        "clean_page",
        "duration_sec_avg_5_dense_rank",
        "cumulative_duration_sec_cume_dist_percent_rank",
        "prev_duration_sec_cume_dist",
    )
    .orderBy(F.col("duration_sec_avg_5_dense_rank").desc())
)
