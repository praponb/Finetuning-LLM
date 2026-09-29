from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(174, 454))
    .select("id", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") != "page_view")
        .select("id", "session_id")
    )
    .distinct()
    .orderBy("id")
)
