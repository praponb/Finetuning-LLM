from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

with_windows = (
    df_web_events
    .select(
        "*",
        F.ntile(4).over(w_hist).alias("duration_sec_bucket"),
        F.lag("duration_sec", 2, 0).over(w2).alias("prev_duration_sec"),
    )
)

result = (
    with_windows
    .withColumn("duration_sec_bucket_tier", F.when(F.col("duration_sec_bucket") >= 8, "HIGH").otherwise("LOW"))
    .cube("event_type", "duration_sec_bucket_tier", "page")
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.sum(F.when(F.col("page") == "/search", F.col("prev_duration_sec")).otherwise(0)).alias("search_prev_duration_sec"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.round(F.avg("duration_sec_bucket"), 2).alias("avg_duration_sec_bucket"),
        F.grouping("event_type").alias("gid"),
    )
    .filter(F.col("num_rows") < 1)
    .orderBy("purchase_rows")
)
