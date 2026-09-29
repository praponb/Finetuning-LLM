from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type")
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w4 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_web_events
    .filter(F.col("page") != "/product")
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.min("duration_sec").over(w2).alias("event_type_min_duration_sec"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
        F.sum("duration_sec").over(w4).alias("cumulative_duration_sec_2"),
        F.sum("duration_sec").over(w5).alias("running_duration_sec"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("cumulative_duration_sec_2")).otherwise(0)).alias("search_cumulative_duration_sec_2"),
        F.count("*").alias("num_rows"),
        F.max("cumulative_duration_sec").alias("max_cumulative_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
)
