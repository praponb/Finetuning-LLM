from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("burst_id", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("event_type")
w6 = Window.partitionBy("page").orderBy("duration_sec", "id")
w7 = Window.partitionBy("event_type", "session_id").orderBy(F.col("new_group_flag").desc(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-05-25 04:00:00"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

scored = (
    islands
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "resolved_page",
        "dedup_rank",
        "new_group_flag",
        "burst_id",
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_6_prior"),
        F.min("burst_id").over(w5).alias("event_type_min_burst_id"),
        F.lag("duration_sec", 1).over(w6).alias("prev_duration_sec"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country"), F.col("b.browser"))
)

result = (
    with_dims
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL"))
    .cube("event_type", "burst_id_tier")
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.percentile_approx("prev_duration_sec", 0.95).alias("prev_duration_sec_p95"),
        F.max("seq_no").alias("max_seq_no"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("de_events").asc_nulls_first())
)
