from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy(F.col("prev_duration_sec").desc())
w_running = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w5 = Window.partitionBy("session_id")
w6 = Window.partitionBy("page")

cleaned = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .withColumn("event_type_page", F.concat("event_type", F.lit(" "), "page"))
)

metrics = (
    cleaned
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

valid = (
    metrics
    .filter(F.col("row_seq") > 3)
    .withColumn("prev_duration_sec_tier", F.when(F.col("prev_duration_sec") >= 436, "High").when(F.col("prev_duration_sec") >= 149, "Medium").otherwise("Low"))
    .withColumn("page_clean", F.coalesce("page", F.lit("DIRECT")))
)

deduped = (
    valid
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

remaining_keys = (
    deduped
    .select("session_id")
    .intersect(
        df_web_events
        .filter(F.col("page").isNull())
        .select("session_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

metrics_web_events = (
    kept_rows
    .select(
        "id",
        "session_id",
        "event_seq",
        "row_seq",
        F.dense_rank().over(w_running).alias("duration_sec_dense_rank"),
        F.sum("duration_sec").over(w5).alias("session_id_sum_duration_sec"),
        F.sum("event_seq").over(w6).alias("page_sum_event_seq"),
    )
)

result = (
    metrics_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.round(F.avg("duration_sec_dense_rank"), 2).alias("avg_duration_sec_dense_rank"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.round(F.avg("duration_sec_dense_rank"), 2).alias("avg_duration_sec_dense_rank_2"),
        F.count("*").alias("row_count"),
        F.round(F.avg("page_sum_event_seq"), 2).alias("avg_page_sum_event_seq"),
    )
    .orderBy(F.col("avg_row_seq").desc())
)
