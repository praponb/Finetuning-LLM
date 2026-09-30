from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "page_view")
    .select("duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") < "2025-10-09")
        .select("duration_sec")
    )
    .distinct()
)
