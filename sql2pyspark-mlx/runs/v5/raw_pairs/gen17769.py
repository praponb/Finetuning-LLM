from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy("event_time", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy("next_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.orderBy(F.col("next_event_seq").desc())

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-03-18 06:00:00").cast("timestamp")) & (F.col("event_type") != "search"))
    .withColumn("page_group", F.when(F.col("page").isin(["/home", "/cart"]), "Core").otherwise("Other"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

scored = (
    filtered
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
)

cleaned = (
    scored
    .filter(F.col("event_time") >= F.lit("2023-03-25 17:00:00").cast("timestamp"))
    .select(
        "*",
        F.round((F.col("event_seq") * 0.9), 2).alias("event_seq_scaled"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.max("prev_duration_sec").over(w3).alias("prev_duration_sec_max_90d"),
        F.lead("event_seq", 1).over(w4).alias("next_event_seq"),
    )
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w5).isNull() | (F.col("event_time") > F.lag("event_time").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w6))

with_windows = (
    islands
    .filter(F.col("seq_no") == 7)
    .select(
        "*",
        F.first("event_seq_scaled", ignorenulls=True).over(w7).alias("first_known_event_seq_scaled"),
        F.avg("seq_no").over(w8).alias("seq_no_avg_2"),
        F.rank().over(w9).alias("next_event_seq_rank"),
    )
)

summary = (
    with_windows
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .groupingSets(
        [["is_new_burst_tier", "seq_no_tier", "week_start"], ["is_new_burst_tier", "seq_no_tier"], ["is_new_burst_tier"], []],
        "is_new_burst_tier",
        "seq_no_tier",
        "week_start",
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("is_new_burst")).otherwise(0)).alias("search_is_new_burst"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    summary
    .select(
        "seq_no_tier",
        "week_start",
        "search_is_new_burst",
        "avg_duration_sec",
        "gid",
    )
    .orderBy(F.col("seq_no_tier").desc_nulls_first())
)
