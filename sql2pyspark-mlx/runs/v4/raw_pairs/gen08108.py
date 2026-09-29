from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2024-04-29").cast("date"))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
    .withColumn("page_clean", F.coalesce("page", F.lit("N/A")))
)

result = (
    cleaned
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") == 248)
        .select("page", "id")
    )
)
