from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .select(
        "*",
        F.first("duration_sec", ignorenulls=True).over(w).alias("first_known_duration_sec"),
        F.count("duration_sec").over(w2).alias("session_id_count_duration_sec"),
        F.last("page", ignorenulls=True).over(w3).alias("last_known_page"),
    )
)

result = (
    enriched
    .cube("event_type", "session_id")
    .agg(
        F.percentile_approx("first_known_duration_sec", 0.5).alias("first_known_duration_sec_p50"),
        F.min("event_time").alias("first_event_time"),
        F.percentile_approx("session_id_count_duration_sec", [0.25, 0.5, 0.75]).alias("session_id_count_duration_sec_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("session_id").asc_nulls_last())
)
