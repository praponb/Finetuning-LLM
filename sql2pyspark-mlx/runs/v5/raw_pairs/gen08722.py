from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("event_type").isin(["page_view", "add_to_cart"]), "priority").otherwise("other").alias("event_type_group"),
        "session_id",
    )
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
    .orderBy(F.col("duration_sec_range").desc(), "event_type_group", "session_id")
    .limit(1)
)
