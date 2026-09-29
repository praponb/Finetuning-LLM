from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_type", "id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2023-09-18")
        .select("event_type", "id")
    )
)
