from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_first(), "id")
w_hist = Window.partitionBy("page")
w5 = Window.orderBy(F.col("duration_sec").desc(), "id")
w_part = Window.orderBy(F.col("duration_sec").desc_nulls_first())

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") > 235))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

deduped = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("duration_sec").over(w_hist).alias("page_sum_duration_sec"),
        F.stddev_samp("session_start_flag").over(w_hist).alias("page_stddev_samp_session_start_flag"),
        F.ntile(10).over(w5).alias("duration_sec_bucket"),
        F.cume_dist().over(w_part).alias("duration_sec_cume_dist"),
    )
)

with_prev = enriched.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag_2", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id_2", F.sum("session_start_flag_2").over(w2))

result = (
    islands
    .select(
        "id",
        "page",
        "duration_sec",
        "event_time_label",
        "session_id",
        "row_num",
        "page_sum_duration_sec",
        "page_stddev_samp_session_start_flag",
        "duration_sec_bucket",
    )
    .orderBy(F.col("page").desc(), "page_sum_duration_sec")
)
