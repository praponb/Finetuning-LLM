from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_type_page_key", F.concat_ws("-", "event_type", "page"))
)

result = (
    cleaned
    .select("session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id").isin([6, 13]))
        .select("session_id")
    )
)
