from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

scored = (
    df_web_events
    .select(
        "*",
        F.ntile(5).over(w).alias("duration_sec_bucket"),
        F.lag("duration_sec", 2, 0).over(w2).alias("prev_duration_sec"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_3"),
        F.last("page", ignorenulls=True).over(w4).alias("last_known_page"),
        F.sum("duration_sec").over(w5).alias("duration_sec_sum_30d"),
    )
)

result = (
    scored
    .groupBy("session_id", F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.min("event_time").alias("first_seen"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(F.avg("duration_sec_avg_3"), 2).alias("avg_duration_sec_avg_3"),
        F.percentile_approx("duration_sec_bucket", 0.5).alias("duration_sec_bucket_p50"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1)
)
