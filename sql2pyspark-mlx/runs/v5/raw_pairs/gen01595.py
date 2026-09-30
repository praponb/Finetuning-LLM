from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country", "device").orderBy(F.col("web_event_duration_sec").desc_nulls_last(), "id")
w5 = Window.partitionBy("country").orderBy("web_event_duration_sec", "id")
w6 = Window.orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("web_event_duration_sec").asc_nulls_first(), "id")
w8 = Window.orderBy(F.col("web_event_duration_sec_bucket").desc())
w9 = Window.partitionBy("country").orderBy(F.col("prev_started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

latest_match = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.started_at",
        "s.country",
        "s.prev_started_at",
        "s.new_group_flag",
        "s.island_id",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_type"),
    )
)

deduped = (
    latest_match
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "device",
        "country",
        "prev_started_at",
        "island_id",
        "web_event_duration_sec",
    )
)

enriched = (
    deduped
    .select(
        "id",
        "country",
        "prev_started_at",
        "web_event_duration_sec",
        F.ntile(5).over(w5).alias("web_event_duration_sec_bucket"),
        F.first("web_event_duration_sec", ignorenulls=True).over(w6).alias("first_known_web_event_duration_sec"),
    )
)

deduped_sessions = (
    enriched
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    deduped_sessions
    .select("id", "country")
    .intersect(
        df_sessions
        .filter(F.col("country").isNull())
        .select("id", "country")
    )
)

verified = (
    deduped_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "country",
        "prev_started_at",
        "web_event_duration_sec_bucket",
        "first_known_web_event_duration_sec",
        F.rank().over(w8).alias("web_event_duration_sec_bucket_rank"),
        F.count("first_known_web_event_duration_sec").over(w9).alias("first_known_web_event_duration_sec_count_7d"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("web_event_duration_sec_bucket") >= 7, "LARGE").otherwise("SMALL").alias("web_event_duration_sec_bucket_tier"),
        F.when(F.col("first_known_web_event_duration_sec") >= 411, "HIGH").otherwise("LOW").alias("first_known_web_event_duration_sec_tier"),
        F.when(F.col("first_known_web_event_duration_sec_count_7d") >= 18, "HIGH").otherwise("LOW").alias("first_known_web_event_duration_sec_count_7d_tier"),
    )
    .agg(
        F.round(F.sum("web_event_duration_sec_bucket") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("web_event_duration_sec_bucket_per_jp"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
