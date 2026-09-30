from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.partitionBy("session_id", "page").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_web_events
    .filter(F.col("session_id").isin([13, 18, 38]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.stddev_samp("duration_sec").over(w).alias("session_id_stddev_samp_duration_sec"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_2_2"),
        F.first("page", ignorenulls=True).over(w4).alias("first_known_page"),
        F.sum("duration_sec").over(w5).alias("cumulative_duration_sec"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("duration_sec_avg_2_2") >= 518, "PREMIUM").otherwise("STANDARD").alias("duration_sec_avg_2_2_tier"),
        F.when(F.col("cumulative_duration_sec") >= 2653, "HIGH").otherwise("LOW").alias("cumulative_duration_sec_tier"),
        "page",
    )
    .agg(
        F.percentile_approx("cumulative_duration_sec", 0.95).alias("cumulative_duration_sec_p95"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("duration_sec_avg_2_2", [0.25, 0.5, 0.75]).alias("duration_sec_avg_2_2_quartiles"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .orderBy(F.col("search_count").asc_nulls_last())
)
