from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.partitionBy("device").orderBy(F.col("duration_sec").asc_nulls_first())
w_ordered = Window.partitionBy("page").orderBy("row_num", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("page", "browser").orderBy("row_num", "id")
w_grp = Window.orderBy("duration_sec_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device").orderBy("duration_sec_dense_rank", "id")
w_part = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w10 = Window.partitionBy("device").orderBy("event_time", "id")
w11 = Window.partitionBy("device").orderBy(F.col("duration_sec_avg_2").asc_nulls_last())
w12 = Window.partitionBy("page").orderBy("duration_sec_dense_rank", "id")

cleaned = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .withColumn("event_type_page_key", F.concat_ws("-", "event_type", "page"))
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.device"), F.col("d.browser"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.sum("duration_sec").over(w2).alias("duration_sec_sum_7d"),
        F.dense_rank().over(w_recent).alias("duration_sec_dense_rank"),
        F.avg("duration_sec").over(w_ordered).alias("duration_sec_avg_2"),
    )
)

metrics = (
    with_windows
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("duration_sec_sum_7d_avg_3", F.avg("duration_sec_sum_7d").over(w_grp))
    .withColumn("last_known_device", F.last("device", ignorenulls=True).over(w7))
    .withColumn("row_seq_2", F.row_number().over(w8))
    .withColumn("event_seq", F.row_number().over(w_part))
)

with_windows_web_events = (
    metrics
    .select(
        "*",
        F.lag("duration_sec_sum_7d_avg_3", 1, 0).over(w10).alias("prev_duration_sec_sum_7d_avg_3"),
        F.percent_rank().over(w11).alias("duration_sec_avg_2_percent_rank"),
    )
)

latest = (
    with_windows_web_events
    .withColumn("latest_rank", F.row_number().over(w12))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy(
        F.when(F.col("duration_sec") >= 537, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.percentile_approx("duration_sec_sum_7d_avg_3", [0.25, 0.5, 0.75]).alias("duration_sec_sum_7d_avg_3_quartiles"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.round(F.avg("duration_sec_dense_rank"), 2).alias("avg_duration_sec_dense_rank"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.max("event_time").alias("ended_at"),
    )
    .filter(F.col("num_pages") >= 1)
    .orderBy(F.col("duration_sec_bit_and").desc())
)
