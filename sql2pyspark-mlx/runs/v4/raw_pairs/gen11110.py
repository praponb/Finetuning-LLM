from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") < 255))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
    )
)

with_prev = filtered.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("is_purchase") >= 70, "LARGE").otherwise("SMALL").alias("is_purchase_tier"),
        F.when(F.col("session_no") >= 9, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.round(F.avg("is_purchase"), 2).alias("avg_is_purchase"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_per_page_view"),
    )
    .filter(F.col("search_events") > 1)
    .orderBy(F.col("avg_session_no").desc())
)
