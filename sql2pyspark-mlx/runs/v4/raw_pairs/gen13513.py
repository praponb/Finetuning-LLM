from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("event_type").orderBy(F.col("prev_event_time").asc_nulls_first(), "id")
w8 = Window.partitionBy("event_type").orderBy(F.col("is_new_burst").desc())
w9 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w10 = Window.partitionBy("event_type")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= "2023-11-27")
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("page_title", F.initcap(F.trim("page")))
)

metrics = (
    cleaned
    .select(
        "*",
        F.first("duration_sec", ignorenulls=True).over(w).alias("first_known_duration_sec"),
        F.ntile(10).over(w2).alias("duration_sec_bucket"),
    )
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("is_new_burst").over(w4))

prev_events = sessions_web_events.withColumn("prev_event_time", F.lag("event_time").over(w5))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w6))

latest = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    latest
    .select("page")
    .exceptAll(
        df_web_events
        .filter(F.to_date("event_time") < "2023-07-01")
        .select("page")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("is_new_burst_cume_dist", F.cume_dist().over(w8))
    .withColumn("duration_sec_max_30d", F.max("duration_sec").over(w9))
    .withColumn("event_type_stddev_samp_duration_sec_bucket", F.stddev_samp("duration_sec_bucket").over(w10))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("event_type_stddev_samp_duration_sec_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_type_stddev_samp_duration_sec_bucket_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        "island_id",
    )
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.min("event_time").alias("first_event_time"),
        F.countDistinct("page").alias("distinct_pages"),
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
