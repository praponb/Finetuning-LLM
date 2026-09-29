from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time_period", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id", "event_type").orderBy(F.col("is_new_burst").desc(), "id")
w5 = Window.partitionBy("session_id").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy(F.col("next_duration_sec").desc(), "id")
w_ordered = Window.partitionBy("page", "event_type").orderBy(F.col("seq_no").desc_nulls_first())
w8 = Window.partitionBy("event_type").orderBy(F.col("island_id").desc())

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-06-12 14:00:00").cast("timestamp"))
    .select("*", F.date_trunc("week", "event_time").alias("event_time_period"))
)

scored = (
    base
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w_running))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
)

gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("event_time_period") - F.lag("event_time_period").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w3))

metrics = (
    islands
    .select(
        "id",
        "event_type",
        "event_time",
        "event_time_period",
        "is_new_burst",
        "island_id",
        F.ntile(3).over(w4).alias("is_new_burst_bucket"),
        F.last("seq_no", ignorenulls=True).over(w5).alias("last_known_seq_no"),
        F.ntile(4).over(w6).alias("next_duration_sec_bucket"),
        F.rank().over(w_ordered).alias("seq_no_rank"),
        F.dense_rank().over(w8).alias("island_id_dense_rank"),
    )
)

matched_keys = (
    metrics
    .select("event_type", "id")
    .exceptAll(
        df_web_events
        .filter(F.col("duration_sec") > 305)
        .select("event_type", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("last_known_seq_no_tier", F.when(F.col("last_known_seq_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["island_id", "last_known_seq_no_tier"], ["island_id"]],
        "island_id",
        "last_known_seq_no_tier",
    )
    .agg(
        F.avg("next_duration_sec_bucket").alias("avg_next_duration_sec_bucket"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.percentile_approx("next_duration_sec_bucket", [0.25, 0.5, 0.75]).alias("next_duration_sec_bucket_quartiles"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("num_event_types") > 1)
    .orderBy("avg_next_duration_sec_bucket")
)
