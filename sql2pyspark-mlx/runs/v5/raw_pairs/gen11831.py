from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_hist = Window.partitionBy("browser", "device")
w3 = Window.orderBy(F.col("browser_max_duration_sec_times_duration_sec_count_90d").desc(), "id")
w4 = Window.orderBy("duration_sec_count_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("duration_sec_count_90d").desc(), "id")
w6 = Window.orderBy("duration_sec", "id")
w7 = Window.orderBy("duration_sec_whole", "id")

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        F.col("b.browser"),
        F.col("b.country"),
        F.col("b.device"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "duration_sec",
        F.count("duration_sec").over(w).alias("duration_sec_count_90d"),
        F.max("duration_sec").over(w_hist).alias("browser_max_duration_sec"),
    )
)

filtered = (
    metrics
    .filter(F.col("browser_max_duration_sec").between(329, 378))
    .select(
        "*",
        F.when(F.col("duration_sec") >= 390, "High").when(F.col("duration_sec") >= 122, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
)

cleaned = (
    filtered
    .filter(F.col("duration_sec_count_90d") >= 12)
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
    .withColumn("browser_max_duration_sec_times_duration_sec_count_90d", (F.col("browser_max_duration_sec") * F.col("duration_sec_count_90d")))
)

enriched_web_events = (
    cleaned
    .withColumn("browser_max_duration_sec_times_duration_sec_count_90d_bucket", F.ntile(10).over(w3))
    .withColumn("last_known_duration_sec_count_90d", F.last("duration_sec_count_90d", ignorenulls=True).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("duration_sec_bucket", F.ntile(3).over(w6))
    .withColumn("prev_browser_max_duration_sec_times_duration_sec_count_90d", F.lag("browser_max_duration_sec_times_duration_sec_count_90d", 2).over(w7))
)

result = (
    enriched_web_events
    .groupBy(
        F.when(F.col("duration_sec_whole") >= 86, "HIGH").otherwise("LOW").alias("duration_sec_whole_tier"),
    )
    .agg(
        F.max("browser_max_duration_sec").alias("max_browser_max_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("distinct_ids_2") >= 1)
)
