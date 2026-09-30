from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "click"]) & ((F.col("duration_sec") == 351) | ~F.col("event_type").isin(["click", "add_to_cart"])))
    .select(
        "id",
        "duration_sec",
        "event_type",
        F.when(F.col("event_type") == "click", 1).otherwise(0).alias("is_click"),
    )
    .orderBy("event_type")
)
