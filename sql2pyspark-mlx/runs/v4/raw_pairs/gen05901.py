from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select("event_type")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([3, 20, 28, 31]))
        .select("event_type")
    )
    .orderBy(F.col("event_type").desc())
)
