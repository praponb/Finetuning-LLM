from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy("page", "session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.max(F.when(F.col("event_type") == "page_view", F.col("is_new_session"))).alias("page_view_max_is_new_session"),
        F.max("event_time").alias("ended_at"),
        F.sum(F.when(F.col("event_type") == "search", F.col("burst_id")).otherwise(0)).alias("search_burst_id"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("duration_min").desc())
)
