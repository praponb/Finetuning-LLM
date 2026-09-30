from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("duration_sec").isNotNull())
    .select(
        "*",
        F.date_trunc("WEEK", "event_time").alias("event_time_period"),
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

result = (
    cleaned
    .groupBy("session_id", "page")
    .pivot("event_type", ["click", "page_view"])
    .agg(F.sum("duration_sec"))
)
