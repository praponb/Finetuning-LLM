from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("page")
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("page").orderBy(F.col("event_time").desc_nulls_first())

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-24 09:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.concat("event_type", F.lit("/"), "page").alias("event_type_page"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec"),
        F.stddev_samp("duration_sec").over(w2).alias("page_stddev_samp_duration_sec"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_3"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_2"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
    .orderBy(F.col("search_count").desc_nulls_last())
)
