from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("event_type", "page")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") < 349)
        .select("event_type", "page")
    )
    .distinct()
)
