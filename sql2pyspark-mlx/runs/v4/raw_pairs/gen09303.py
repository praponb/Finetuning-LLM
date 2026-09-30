from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_part = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_last())
w5 = Window.orderBy("duration_sec_dense_rank", "id")
w6 = Window.partitionBy("page", "device").orderBy(F.col("duration_sec_count_30d").desc(), "id")

cleaned = (
    df_web_events
    .filter(F.hour("event_time").between(10, 13))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("page_clean", F.coalesce("page", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .withColumn("duration_sec_avg_6", F.avg("duration_sec").over(w))
    .withColumn("duration_sec_count_30d", F.count("duration_sec").over(w2))
    .withColumn("duration_sec_avg_6_2", F.avg("duration_sec").over(w_part))
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w4))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.device"))
)

result = (
    enriched
    .select(
        "*",
        F.lag("duration_sec_avg_6", 1).over(w5).alias("prev_duration_sec_avg_6"),
        F.sum("duration_sec_dense_rank").over(w2).alias("duration_sec_dense_rank_sum_30d"),
        F.row_number().over(w6).alias("row_seq"),
    )
)
