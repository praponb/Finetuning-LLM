from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

with_windows = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-05-13")
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.max("duration_sec").over(w2).alias("duration_sec_max_90d"),
        F.sum("duration_sec").over(w3).alias("duration_sec_sum_30d"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("month", "event_time").alias("activity_month"))
    .agg(
        F.percentile_approx("duration_sec_max_90d", 0.95).alias("duration_sec_max_90d_p95"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.countDistinct("page").alias("distinct_pages"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("duration_sec_max_90d"), 2).alias("avg_duration_sec_max_90d"),
    )
    .orderBy(F.col("duration_sec_max_90d_p95").desc())
)
