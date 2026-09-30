from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w_running = Window.partitionBy("page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("event_type", "session_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w9 = Window.partitionBy("session_id")
w10 = Window.orderBy(F.col("duration_sec_bucket").desc_nulls_first())
w11 = Window.partitionBy("event_type", "session_id").orderBy("cumulative_duration_sec")
w12 = Window.orderBy(F.col("duration_sec_rank").desc_nulls_last())

with_windows = (
    df_web_events
    .withColumn("duration_sec_rank", F.rank().over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_running))
)

with_gaps = (
    with_windows
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w4))

base = (
    sessions_web_events
    .filter((F.col("event_time") > F.lit("2023-11-14").cast("date")) & F.col("page").isNotNull() & (F.col("is_new_burst") != 1))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "duration_sec_rank",
        "seq_no",
        "island_id",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

scored = (
    base
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w5))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w6))
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w7))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w_grp))
)

base_web_events = (
    scored
    .filter(F.col("cumulative_duration_sec") >= 1809)
    .select(
        "*",
        (F.col("island_id") + F.col("cumulative_duration_sec")).alias("island_id_plus_cumulative_duration_sec"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean_2"),
    )
)

remaining_keys = (
    base_web_events
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([20, 26, 38, 40]))
        .select("session_id")
    )
)

reconciled = (
    base_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

scored_web_events = (
    reconciled
    .select(
        "*",
        F.stddev_samp("seq_no").over(w9).alias("session_id_stddev_samp_seq_no"),
        F.cume_dist().over(w10).alias("duration_sec_bucket_cume_dist"),
        F.dense_rank().over(w11).alias("cumulative_duration_sec_dense_rank"),
        F.percent_rank().over(w12).alias("duration_sec_rank_percent_rank"),
    )
)

result = (
    scored_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "duration_sec_rank",
        "island_id",
        "duration_sec_num",
        "page_clean",
        "first_known_page",
        F.when(F.col("event_type").rlike("^(purchase|page_view)$"), "PRIORITY").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("island_id").desc())
)
