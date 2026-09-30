from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(9, 13))
    .select("id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") == 249)
        .select("id", "event_type")
    )
    .distinct()
    .orderBy("id")
)
