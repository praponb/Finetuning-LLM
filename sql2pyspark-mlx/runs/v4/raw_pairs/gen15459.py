from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
)

filtered = (
    valid
    .filter(F.col("event_time") > F.to_timestamp(F.lit("2024-03-24 18:00:00")))
    .select(
        "*",
        F.length("event_type").alias("event_type_length"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

base = (
    filtered
    .filter(F.col("duration_sec") > 172)
    .select(
        "*",
        F.upper("page").alias("page_upper"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

filtered_web_events = (
    base
    .filter(F.col("event_time") < "2024-04-09")
    .select(
        "*",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean_2"),
    )
)

result = (
    filtered_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        "event_time_label",
        "event_type_length",
        "page_clean",
        "page_upper",
        "page_filled",
        "page_clean_2",
    )
    .orderBy("id", "event_time")
    .limit(10)
)
