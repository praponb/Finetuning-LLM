from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc())

with_windows = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("page_avg_duration_sec"),
        F.percent_rank().over(w2).alias("duration_sec_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy("event_type")
    .agg(
        F.sum("duration_sec_percent_rank").alias("total_duration_sec_percent_rank"),
        F.avg(F.when(F.col("page") == "/product", F.col("duration_sec"))).alias("product_avg_duration_sec"),
        F.percentile_approx("page_avg_duration_sec", 0.95).alias("page_avg_duration_sec_p95"),
    )
    .filter(F.col("page_avg_duration_sec_p95") > 0)
)
