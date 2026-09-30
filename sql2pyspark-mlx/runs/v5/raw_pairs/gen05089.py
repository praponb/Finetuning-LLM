from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec_rounded", "id")
w2 = Window.partitionBy("session_id").orderBy("rank_in_group", "id")
w3 = Window.partitionBy("event_type", "page").orderBy("duration_sec_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("duration_sec_rounded", "id")
w5 = Window.orderBy("next_duration_sec_rounded", "id").rowsBetween(-2, -1)
w_ordered = Window.orderBy(F.col("duration_sec_rounded").desc_nulls_last())
w7 = Window.partitionBy("duration_sec_rounded_tier").orderBy(F.col("distinct_ids").desc())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped_web_events = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped_web_events
    .select(
        "id",
        "duration_sec_rounded",
        F.last("page", ignorenulls=True).over(w3).alias("last_known_page"),
        F.lead("duration_sec_rounded", 1, 0).over(w4).alias("next_duration_sec_rounded"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "duration_sec_rounded",
        "last_known_page",
        F.avg("next_duration_sec_rounded").over(w5).alias("next_duration_sec_rounded_avg_2_prior"),
        F.dense_rank().over(w_ordered).alias("duration_sec_rounded_dense_rank"),
    )
)

stats = (
    metrics
    .groupBy(
        F.when(F.col("duration_sec_rounded") >= 67, "LARGE").otherwise("SMALL").alias("duration_sec_rounded_tier"),
        F.when(F.col("next_duration_sec_rounded_avg_2_prior") >= 78, "LARGE").otherwise("SMALL").alias("next_duration_sec_rounded_avg_2_prior_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("duration_sec_rounded"), 2).alias("avg_duration_sec_rounded"),
        F.round(F.avg("next_duration_sec_rounded_avg_2_prior"), 2).alias("avg_next_duration_sec_rounded_avg_2_prior"),
    )
    .filter(F.col("avg_next_duration_sec_rounded_avg_2_prior") < 1000000)
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w7))
)

result = (
    stats
    .select(
        "duration_sec_rounded_tier",
        "next_duration_sec_rounded_avg_2_prior_tier",
        "distinct_ids",
        "avg_duration_sec_rounded",
        "avg_next_duration_sec_rounded_avg_2_prior",
    )
    .orderBy(
        F.col("next_duration_sec_rounded_avg_2_prior_tier").asc_nulls_last(),
        "avg_next_duration_sec_rounded_avg_2_prior",
    )
)
