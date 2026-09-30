from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") == "search"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("duration_sec") >= 404, "High").when(F.col("duration_sec") >= 65, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

result = (
    base
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([9, 17, 30]))
        .select("id")
    )
)
