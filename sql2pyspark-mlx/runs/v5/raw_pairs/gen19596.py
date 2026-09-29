from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w_grp = Window.partitionBy("event_type").orderBy(F.col("duration_sec").asc_nulls_first())
w_ordered = Window.partitionBy("device", "session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("browser").orderBy("duration_sec", "id")
w5 = Window.partitionBy("device")
w6 = Window.partitionBy("session_id", "device").orderBy(F.col("next_duration_sec").desc(), "id")
w7 = Window.partitionBy("page").orderBy("event_seq", "id")
w8 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("browser").orderBy("prev_duration_sec", "id").rowsBetween(-5, -1)
w10 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())

metrics = (
    df_web_events
    .withColumn("next_duration_sec", F.lead("duration_sec", 1, 0).over(w))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w_grp))
)

enriched = (
    metrics
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.duration_sec",
        F.col("p.browser"),
        F.col("p.device"),
    )
)

metrics_web_events = (
    enriched
    .select(
        "*",
        F.lag("duration_sec").over(w_ordered).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w_ordered).alias("next_duration_sec"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.ntile(5).over(w4).alias("duration_sec_bucket"),
    )
)

metrics_web_events_2 = (
    metrics_web_events
    .withColumn("device_sum_next_duration_sec", F.sum("next_duration_sec").over(w5))
    .withColumn("next_duration_sec_bucket", F.ntile(10).over(w6))
    .withColumn("prev_event_seq", F.lag("event_seq", 1, 0).over(w7))
)

with_windows = (
    metrics_web_events_2
    .filter(F.col("prev_event_seq") != 3)
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w8))
    .withColumn("next_duration_sec_avg_5_prior", F.avg("next_duration_sec").over(w_hist))
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w10))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("next_duration_sec_bucket") >= 7, "LARGE").otherwise("SMALL").alias("next_duration_sec_bucket_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.max(F.when(F.col("page") == "/home", F.col("event_seq"))).alias("home_max_event_seq"),
        F.min("next_duration_sec_avg_5_prior").alias("min_next_duration_sec_avg_5_prior"),
    )
    .orderBy(F.col("duration_min").asc_nulls_first())
)
