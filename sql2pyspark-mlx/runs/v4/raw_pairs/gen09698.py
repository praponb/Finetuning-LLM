from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.min("session_id_2").alias("min_session_id_2"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.avg(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec"))).alias("add_to_cart_avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec"))).alias("search_total_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("search_total_duration_sec") >= 0)
    .orderBy(F.col("add_to_cart_avg_duration_sec").asc_nulls_first())
)
