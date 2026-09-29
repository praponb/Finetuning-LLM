from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

with_windows = (
    df_web_events
    .select(
        "*",
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        "session_id",
        "page",
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.avg(F.when(F.col("event_type") == "search", F.col("event_seq"))).alias("search_avg_event_seq"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_per_cart"),
        F.count("*").alias("row_count"),
    )
)
