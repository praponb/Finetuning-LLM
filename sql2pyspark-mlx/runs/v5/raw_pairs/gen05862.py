from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_web_events
    .rollup(F.date_trunc("week", "event_time").alias("week_start"))
    .agg(
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.sum(F.when(F.col("event_type") == "click", F.col("burst_id"))).alias("click_total_burst_id"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
    )
)
