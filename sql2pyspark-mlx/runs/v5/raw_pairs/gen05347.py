from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "click") & F.col("page").isNull() & (F.col("event_type") != "add_to_cart"))
    .select(
        "session_id",
        "page",
        "event_time",
        F.upper("event_type").alias("event_type_upper"),
        F.when(F.col("event_type") == "click", 1).otherwise(0).alias("is_click"),
    )
)
