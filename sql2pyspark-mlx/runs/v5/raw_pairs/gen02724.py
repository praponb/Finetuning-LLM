from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-01-01")
    .select("id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("page") == "/product")
        .select("id", "event_type")
    )
)
