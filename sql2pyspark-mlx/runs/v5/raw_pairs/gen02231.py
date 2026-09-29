from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-07-14").cast("date"))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.weekofyear("event_time").alias("event_time_weekofyear"),
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

result = (
    filtered
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") < 237)
        .select("page", "id")
    )
)
