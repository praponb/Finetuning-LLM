from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-6, -1)

with_windows = (
    df_web_events
    .select(
        "*",
        F.row_number().over(w_recent).alias("event_seq"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.ntile(10).over(w_recent).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_6_prior"),
    )
)

result = (
    with_windows
    .groupBy("page")
    .agg(
        F.round(F.sum("cumulative_duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("cumulative_duration_sec_per_click"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.count("*").alias("cnt"),
        F.max("event_time").alias("last_seen"),
        F.round(F.sum("duration_sec_bucket") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("duration_sec_bucket_per_add_to_cart"),
    )
)
