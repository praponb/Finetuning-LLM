from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 34) & (F.col("session_id") == 7) & F.col("session_id").isin([4, 7, 21, 25]))
    .select(
        "id",
        "event_time",
        F.when(F.col("event_type").isin(["add_to_cart", "page_view"]), "Priority").otherwise("Other").alias("event_type_group"),
        F.col("duration_sec").cast("double").alias("duration_sec_str"),
    )
)
