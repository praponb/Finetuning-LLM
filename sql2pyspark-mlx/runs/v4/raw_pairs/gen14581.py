from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("event_type", "page")

with_windows = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_2"),
        F.sum("duration_sec").over(w2).alias("event_type_sum_duration_sec"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_type_sum_duration_sec") >= 2430, "LARGE").otherwise("SMALL").alias("event_type_sum_duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("duration_sec_avg_2") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("duration_sec_avg_2_per_home"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.round(F.avg("duration_sec_avg_2"), 2).alias("avg_duration_sec_avg_2"),
        F.min("event_time").alias("first_event_time"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("duration_sec_avg_2_per_home") > 0)
)
