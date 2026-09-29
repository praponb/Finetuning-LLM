from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w5 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec_rank").desc_nulls_last(), "id")
w6 = Window.partitionBy("session_id", "page").orderBy("duration_sec_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("page").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("duration_sec_rank_whole_tier").orderBy(F.col("cnt").desc())

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-02").cast("date")) & ~F.col("page").isin(["/cart", "/home"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "page_event_type_key",
        "page_clean",
    )
)

scored = (
    deduped
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "page_event_type_key",
        F.lag("duration_sec", 1, 0).over(w2).alias("prev_duration_sec"),
        F.rank().over(w3).alias("duration_sec_rank"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "session_id",
        "page",
        "page_event_type_key",
        "duration_sec_rank",
        "seq_no",
    )
)

scored_web_events = (
    ranked
    .filter(F.col("seq_no").between(6, 8))
    .select(
        "*",
        F.min("seq_no").over(w6).alias("running_min_seq_no"),
        F.max("seq_no").over(w6).alias("running_max_seq_no"),
        F.avg("seq_no").over(w6).alias("running_avg_seq_no"),
        F.last("duration_sec_rank", ignorenulls=True).over(w7).alias("last_known_duration_sec_rank"),
    )
)

valid_web_events = (
    scored_web_events
    .filter(F.col("page").isin(["/product", "/search"]))
    .select(
        "id",
        "session_id",
        "page",
        "page_event_type_key",
        "duration_sec_rank",
        "seq_no",
        "running_min_seq_no",
        "running_max_seq_no",
        "running_avg_seq_no",
        "last_known_duration_sec_rank",
        F.ceil("duration_sec_rank").alias("duration_sec_rank_whole"),
    )
)

cleaned = (
    valid_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.greatest("duration_sec_rank", "running_max_seq_no").alias("greatest_duration_sec_rank_running_max_seq_no"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.when(F.col("duration_sec_rank_whole") >= 84, "LARGE").otherwise("SMALL").alias("duration_sec_rank_whole_tier"),
        "session_id",
        "seq_no",
    )
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_rows"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec_rank")).otherwise(0)).alias("product_duration_sec_rank"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
    )
    .withColumn("cnt_percent_rank", F.percent_rank().over(w8))
)

result = (
    grouped
    .select(
        "duration_sec_rank_whole_tier",
        "session_id",
        "seq_no",
        "cnt",
        "page_list",
        "cnt_percent_rank",
    )
)
