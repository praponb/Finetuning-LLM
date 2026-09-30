from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)
