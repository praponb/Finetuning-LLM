from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("id", "duration_sec", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("page") == "/search")
        .select("id", "duration_sec", "event_type")
    )
    .distinct()
    .orderBy("event_type")
)
