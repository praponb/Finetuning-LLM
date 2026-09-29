from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("page", "session_id").orderBy("event_time", "id")
w_ordered = Window.partitionBy("event_type")
w6 = Window.partitionBy("event_type").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id", "event_type").orderBy("event_type_stddev_samp_duration_sec", "id")
w8 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w10 = Window.partitionBy("session_id")

deduped = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    deduped
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("is_new_burst").over(w3))

enriched = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "rank_in_group",
        "is_new_burst",
        "session_no",
        F.lead("is_new_burst").over(w_recent).alias("next_is_new_burst"),
        F.lag("is_new_burst").over(w_recent).alias("prev_is_new_burst"),
        F.lag("session_no").over(w_recent).alias("prev_session_no"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.stddev_samp("duration_sec").over(w_ordered).alias("event_type_stddev_samp_duration_sec"),
    )
)

enriched_web_events = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "rank_in_group",
        "is_new_burst",
        "prev_is_new_burst",
        "prev_session_no",
        "event_seq",
        F.sum("event_type_stddev_samp_duration_sec").over(w6).alias("cumulative_event_type_stddev_samp_duration_sec"),
        F.lead("event_type_stddev_samp_duration_sec", 1).over(w7).alias("next_event_type_stddev_samp_duration_sec"),
    )
)

matched_keys = (
    enriched_web_events
    .select("session_id")
    .intersect(
        df_web_events
        .filter(F.col("event_type").isin(["search", "add_to_cart", "page_view"]))
        .select("session_id")
    )
)

reconciled = (
    enriched_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("event_seq_sum_30d", F.sum("event_seq").over(w8))
    .withColumn("prev_is_new_burst_avg_90d", F.avg("prev_is_new_burst").over(w9))
    .withColumn("session_id_max_event_seq", F.max("event_seq").over(w10))
)

result = (
    with_windows
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .groupingSets(
        [["device", "event_type"], ["device"]],
        F.col("d.device").alias("device"),
        F.col("a.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.prev_is_new_burst")).alias("min_prev_is_new_burst"),
    )
)
