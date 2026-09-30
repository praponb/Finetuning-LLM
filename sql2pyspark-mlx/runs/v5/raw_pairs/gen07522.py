from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "browser").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w5 = Window.orderBy("duration_sec", "id")
w6 = Window.orderBy("last_known_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("duration_sec_dense_rank", "id").rowsBetween(-6, -1)
w_grp = Window.orderBy("cumulative_duration_sec_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id") == 39)
    .select("a.*", F.col("p.browser"))
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "event_type", "duration_sec")
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "duration_sec")
)

with_windows = (
    latest
    .select(
        "id",
        "duration_sec",
        F.last("duration_sec", ignorenulls=True).over(w3).alias("last_known_duration_sec"),
        F.dense_rank().over(w4).alias("duration_sec_dense_rank"),
        F.lag("duration_sec", 1).over(w5).alias("prev_duration_sec"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "duration_sec",
        "last_known_duration_sec",
        "duration_sec_dense_rank",
        F.sum("duration_sec_dense_rank").over(w6).alias("cumulative_duration_sec_dense_rank"),
        F.avg("last_known_duration_sec").over(w7).alias("last_known_duration_sec_avg_6_prior"),
        F.sum("duration_sec_dense_rank").over(w3).alias("cumulative_duration_sec_dense_rank_2"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "id",
        "duration_sec_dense_rank",
        "last_known_duration_sec_avg_6_prior",
        "cumulative_duration_sec_dense_rank_2",
        F.max("last_known_duration_sec_avg_6_prior").over(w_grp).alias("running_max_last_known_duration_sec_avg_6_prior"),
        F.sum("last_known_duration_sec_avg_6_prior").over(w_grp).alias("running_sum_last_known_duration_sec_avg_6_prior"),
        F.count("last_known_duration_sec_avg_6_prior").over(w_grp).alias("running_count_last_known_duration_sec_avg_6_prior"),
        F.avg("last_known_duration_sec_avg_6_prior").over(w_grp).alias("running_avg_last_known_duration_sec_avg_6_prior"),
    )
)

result = (
    metrics_web_events
    .select(
        "id",
        "duration_sec_dense_rank",
        "cumulative_duration_sec_dense_rank_2",
        "running_avg_last_known_duration_sec_avg_6_prior",
    )
    .orderBy(F.col("cumulative_duration_sec_dense_rank_2").asc_nulls_last())
)
