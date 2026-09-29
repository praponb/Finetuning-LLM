from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 339)
    .select("session_id")
    .unionByName(
        df_web_events
        .filter(F.to_date("event_time") < "2023-10-01")
        .select("session_id")
    )
    .distinct()
    .orderBy("session_id")
)
