from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-03-01")
    .select(
        "id",
        "event_type",
        F.concat("event_type", F.lit("/"), "page").alias("event_type_page"),
    )
    .orderBy("id")
)
