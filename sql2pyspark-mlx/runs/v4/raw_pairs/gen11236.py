from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "click") & (F.col("session_id") == 26) & F.col("event_type").isin(["purchase", "search"]))
    .select(
        "id",
        "event_type",
        F.concat("event_type", F.lit(" - "), "page").alias("event_type_page"),
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
    .orderBy(F.col("id").desc(), "event_type")
)
