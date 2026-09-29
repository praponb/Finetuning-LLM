from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "click") | F.col("session_id").isin([2, 15, 31]))
    .select(
        "id",
        "event_time",
        F.when(F.col("duration_sec") >= 470, "High").when(F.col("duration_sec") >= 122, "Medium").otherwise("Low").alias("duration_sec_level"),
        F.coalesce("page", F.lit("unassigned")).alias("page_filled"),
    )
)
