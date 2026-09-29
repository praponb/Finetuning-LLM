from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        "page",
    )
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("session_no_per_page_view"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
    .orderBy(F.col("quarter_start").desc())
)
