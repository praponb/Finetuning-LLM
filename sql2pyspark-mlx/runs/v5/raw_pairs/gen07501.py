from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("event_time", "id")
w2 = Window.partitionBy("page", "event_type").orderBy(F.col("prev_event_time_hour").asc_nulls_last(), "id")
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time_hour").desc_nulls_last(), "id")
w4 = Window.partitionBy("event_type").orderBy("seq_no")
w5 = Window.partitionBy("event_type").orderBy(F.col("prev_event_time_hour_per_click").desc())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.trim("event_type").alias("clean_event_type"),
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("event_time_hour").between(61, 74))
    .select(
        "*",
        F.lag("event_time_hour").over(w).alias("prev_event_time_hour"),
        F.row_number().over(w).alias("seq_no"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "event_type",
        "clean_event_type",
        "event_time_hour",
        "prev_event_time_hour",
        "seq_no",
    )
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        "event_type",
        "seq_no",
        F.when(F.col("event_time_hour") >= 73, "LARGE").otherwise("SMALL").alias("event_time_hour_tier"),
    )
    .agg(
        F.avg("event_time_hour").alias("avg_event_time_hour"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.percentile_approx("prev_event_time_hour", 0.95).alias("prev_event_time_hour_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("prev_event_time_hour") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("prev_event_time_hour_per_click"),
    )
    .withColumn("prev_event_time_hour_per_click_dense_rank", F.dense_rank().over(w5))
    .orderBy(F.col("seq_no").desc_nulls_last())
)
