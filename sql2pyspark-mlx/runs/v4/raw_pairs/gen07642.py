from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w5 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy("duration_sec_count_90d")
w7 = Window.partitionBy("session_id").orderBy("duration_sec_rounded_max_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("session_id").orderBy("event_time", "id")
w9 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("session_id")
w11 = Window.partitionBy("page").orderBy(F.col("duration_sec_count_90d_rank").desc(), "id")
w12 = Window.partitionBy("page", "event_type")

cleaned = (
    df_web_events
    .filter(F.col("duration_sec") >= 349)
    .withColumn("duration_sec_rounded", F.round("duration_sec", 0))
)

metrics = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("duration_sec_rounded").over(w2).alias("duration_sec_rounded_sum_7d"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_90d"),
        F.avg("duration_sec_rounded").over(w4).alias("duration_sec_rounded_avg_2_prior"),
        F.max("duration_sec_rounded").over(w5).alias("duration_sec_rounded_max_7d"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "*",
        F.rank().over(w6).alias("duration_sec_count_90d_rank"),
        F.first("duration_sec_rounded_sum_7d", ignorenulls=True).over(w7).alias("first_known_duration_sec_rounded_sum_7d"),
    )
)

with_prev = metrics_web_events.withColumn("prev_event_time", F.lag("event_time").over(w8))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w9))

enriched = (
    sessionized
    .withColumn("session_id_count_duration_sec_rounded_sum_7d", F.count("duration_sec_rounded_sum_7d").over(w10))
    .withColumn("duration_sec_count_90d_rank_bucket", F.ntile(10).over(w11))
)

result = (
    enriched
    .select(
        "*",
        F.sum("row_seq").over(w7).alias("running_row_seq"),
        F.max("duration_sec").over(w12).alias("page_max_duration_sec"),
    )
)
