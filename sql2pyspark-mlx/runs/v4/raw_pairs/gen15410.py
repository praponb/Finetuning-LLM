from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/home", "/checkout"]))
    .select("id", "event_type", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") < "2024-07-01")
        .select("id", "event_type", "session_id")
    )
    .distinct()
    .orderBy("session_id")
)
