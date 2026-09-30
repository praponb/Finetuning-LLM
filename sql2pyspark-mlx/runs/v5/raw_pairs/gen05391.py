from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

metrics = (
    df_web_events
    .filter(F.col("event_time") >= "2025-06-01")
    .select(
        "*",
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec"),
        F.ntile(10).over(w2).alias("duration_sec_bucket"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_90d"),
    )
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "last_known_duration_sec",
        "duration_sec_count_90d",
        F.when(F.col("event_type").rlike("^(click|search)$"), "PRIORITY").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("event_type_zone"),
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("event_time").desc(), "event_type_zone")
)
