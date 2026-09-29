from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        "page",
        F.when(F.col("event_type").isin(["search", "purchase"]), "tier_1").otherwise("other").alias("event_type_group"),
    )
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
