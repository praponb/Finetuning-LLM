from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("duration_sec_bucket", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-08 01:00:00").cast("timestamp"))
    .select("*", F.concat_ws(",", "event_type", "page").alias("event_type_page_key"))
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec"),
        F.count("duration_sec").over(w2).alias("duration_sec_count_30d"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_6_prior"),
        F.ntile(5).over(w4).alias("duration_sec_bucket"),
    )
)

enriched_web_events = (
    enriched
    .withColumn("duration_sec_bucket_sum_90d", F.sum("duration_sec_bucket").over(w5))
    .withColumn("duration_sec_count_30d_2", F.count("duration_sec").over(w2))
    .withColumn("duration_sec_avg_6_prior_avg_6_prior", F.avg("duration_sec_avg_6_prior").over(w6))
)

cleaned = (
    enriched_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("duration_sec_avg_6_prior_avg_6_prior_tier", F.when(F.col("duration_sec_avg_6_prior_avg_6_prior") >= 370, "High").when(F.col("duration_sec_avg_6_prior_avg_6_prior") >= 190, "Medium").otherwise("Low"))
)

valid = (
    cleaned
    .filter((F.col("event_time") > F.to_date(F.lit("2024-04-01"))) & F.col("page").isNotNull())
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w7))

wide = (
    sessions_web_events
    .groupBy("session_id")
    .pivot("event_type", ["purchase", "search"])
    .agg(F.max("burst_id"))
)

result = (
    wide
    .select("session_id", "purchase", "search")
    .orderBy(F.col("search").desc())
)
