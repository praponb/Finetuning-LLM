from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("country", "event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w5 = Window.partitionBy("device", "session_id").orderBy("duration_sec_bucket")
w6 = Window.partitionBy("session_id", "event_type")
w7 = Window.partitionBy("country")
w8 = Window.partitionBy("event_type", "page")
w_running = Window.partitionBy("device").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("session_id")
w11 = Window.partitionBy("session_id").orderBy("duration_sec_bucket_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w12 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_count_30d").asc_nulls_last(), "id")

scored = (
    df_web_events
    .select(
        "*",
        F.ntile(5).over(w).alias("duration_sec_bucket"),
        F.count("duration_sec").over(w2).alias("duration_sec_count_30d"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.browser"), F.col("p.country"), F.col("p.device"))
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w5).alias("duration_sec_bucket_dense_rank"),
        F.stddev_samp("seq_no").over(w6).alias("session_id_stddev_samp_seq_no"),
    )
)

scored_web_events = (
    with_windows
    .withColumn("country_count_seq_no", F.count("seq_no").over(w7))
    .withColumn("event_type_max_duration_sec_bucket", F.max("duration_sec_bucket").over(w8))
)

metrics = (
    scored_web_events
    .withColumn("last_known_duration_sec_count_30d", F.last("duration_sec_count_30d", ignorenulls=True).over(w_running))
    .withColumn("duration_sec_bucket_dense_rank_share", F.round((F.col("duration_sec_bucket_dense_rank") / F.sum("duration_sec_bucket_dense_rank").over(w10)), 4))
    .withColumn("duration_sec_bucket_avg_6", F.avg("duration_sec_bucket").over(w11))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    latest
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.percentile_approx("country_count_seq_no", 0.9).alias("country_count_seq_no_p90"),
        F.max("event_time").alias("ended_at"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    stats
    .select("activity_month", "year_start", "country_count_seq_no_p90", "span_minutes")
    .orderBy("year_start")
)
