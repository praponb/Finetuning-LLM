from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "click"]))
    .withColumn("page_event_type", F.concat("page", F.lit(", "), "event_type"))
)

result = (
    filtered
    .select("id", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id").isin([8, 9, 35]))
        .select("id", "event_type")
    )
)
