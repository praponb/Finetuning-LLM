from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")
w2 = Window.orderBy("duration_sec_filled", "id")
w3 = Window.partitionBy("event_type").orderBy("next_duration_sec", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("seq_no", "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("prev_duration_sec").desc(), "id")
w6 = Window.partitionBy("event_type").orderBy(F.col("seq_no").asc_nulls_last())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") != "add_to_cart"))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        F.substring("page", 1, 2).alias("page_prefix"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

base = (
    filtered
    .filter(F.col("duration_sec") >= 384)
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page_2"),
    )
)

with_windows = (
    base
    .filter(F.col("duration_sec").between(279, 397))
    .select(
        "id",
        "event_type",
        "resolved_page_2",
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w).alias("next_duration_sec"),
        F.lag("duration_sec_filled").over(w).alias("prev_duration_sec_filled"),
        F.lead("duration_sec_filled").over(w).alias("next_duration_sec_filled"),
        F.row_number().over(w).alias("seq_no"),
        F.ntile(4).over(w2).alias("duration_sec_filled_bucket"),
    )
)

with_windows_web_events = (
    with_windows
    .select(
        "id",
        "event_type",
        "prev_duration_sec",
        "seq_no",
        "duration_sec_filled_bucket",
        F.avg("prev_duration_sec_filled").over(w3).alias("prev_duration_sec_filled_avg_3"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)

latest = (
    with_windows_web_events
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .rollup(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        "event_type",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(F.col("search_count").asc_nulls_first())
)
