from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("session_id", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy("running_min_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy(F.col("running_avg_duration_sec").desc_nulls_first())
w5 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy("device").orderBy(F.col("running_avg_duration_sec_cume_dist").desc_nulls_last())
w7 = Window.partitionBy("page").orderBy(F.col("running_avg_duration_sec_cume_dist").desc(), "id")
w8 = Window.partitionBy("session_id", "country")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-25 03:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("event_type").alias("event_type_lower"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.count("duration_sec").over(w_ordered).alias("running_count_duration_sec"),
        F.avg("duration_sec").over(w_ordered).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w_ordered).alias("running_min_duration_sec"),
        F.first("page", ignorenulls=True).over(w_part).alias("first_known_page"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.cume_dist().over(w4).alias("running_avg_duration_sec_cume_dist"),
        F.sum("running_min_duration_sec").over(w5).alias("running_min_duration_sec_sum_30d"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("s.started_at")).between(9, 17))
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.duration_sec",
        "a.event_type_lower",
        "a.running_avg_duration_sec",
        "a.first_known_page",
        "a.cumulative_duration_sec",
        "a.running_avg_duration_sec_cume_dist",
        "a.running_min_duration_sec_sum_30d",
        F.col("s.device"),
        F.col("s.country"),
    )
)

scored = (
    with_dims
    .withColumn("running_avg_duration_sec_cume_dist_cume_dist", F.cume_dist().over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
    .withColumn("session_id_stddev_samp_running_avg_duration_sec_cume_dist", F.stddev_samp("running_avg_duration_sec_cume_dist").over(w8))
)

result = (
    scored
    .alias("src")
    .join(df_sessions.alias("dim"), F.col("src.session_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["session_country", "web_event_country"], ["session_country"], []],
        F.col("dim.country").alias("session_country"),
        F.col("src.country").alias("web_event_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.running_min_duration_sec_sum_30d")).alias("max_running_min_duration_sec_sum_30d"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("web_event_country").asc_nulls_first())
)
