from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2023-07-01")
    .select(
        "page",
        "event_type",
        "event_time",
        F.concat("event_type", F.lit("/"), "page").alias("event_type_page"),
    )
)
