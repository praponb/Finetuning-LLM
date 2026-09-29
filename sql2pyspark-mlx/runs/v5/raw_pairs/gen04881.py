from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.orderBy("event_time", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("page", "event_type")
w6 = Window.partitionBy("event_type")
w7 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec_bucket_avg_5_prior").asc_nulls_last())
w8 = Window.partitionBy("page", "event_type").orderBy("duration_sec_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("page").orderBy(F.col("duration_sec_bucket_avg_5_prior").desc(), "id")

enriched = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec"),
        F.ntile(4).over(w2).alias("duration_sec_bucket"),
        F.ntile(3).over(w3).alias("duration_sec_bucket_2"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "event_type",
        "page",
        "last_known_duration_sec",
        "duration_sec_bucket_2",
        F.avg("duration_sec_bucket").over(w4).alias("duration_sec_bucket_avg_5_prior"),
        F.round((F.col("duration_sec_bucket_2") / F.sum("duration_sec_bucket_2").over(w5)), 4).alias("duration_sec_bucket_2_share"),
    )
)

enriched_web_events = (
    with_windows
    .withColumn("event_type_sum_last_known_duration_sec", F.sum("last_known_duration_sec").over(w6))
    .withColumn("duration_sec_bucket_avg_5_prior_cume_dist", F.cume_dist().over(w7))
    .withColumn("cumulative_duration_sec_bucket_avg_5_prior", F.sum("duration_sec_bucket_avg_5_prior").over(w8))
)

latest = (
    enriched_web_events
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "page",
        "last_known_duration_sec",
        "duration_sec_bucket_2",
        "duration_sec_bucket_avg_5_prior",
        "duration_sec_bucket_2_share",
        "event_type_sum_last_known_duration_sec",
        "duration_sec_bucket_avg_5_prior_cume_dist",
        "cumulative_duration_sec_bucket_avg_5_prior",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
