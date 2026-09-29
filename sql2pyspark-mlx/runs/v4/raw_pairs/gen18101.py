from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_web_events
    .select(
        "*",
        F.last("page", ignorenulls=True).over(w_ordered).alias("last_known_page"),
        F.sum("duration_sec").over(w_ordered).alias("running_duration_sec"),
    )
)

result = (
    metrics
    .groupBy(
        "event_type",
        "page",
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.max("event_time").alias("ended_at"),
        F.percentile_approx("running_duration_sec", 0.95).alias("running_duration_sec_p95"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.count("*").alias("num_rows"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("ended_at_2"),
    )
    .orderBy(F.col("event_type").desc_nulls_last())
)
