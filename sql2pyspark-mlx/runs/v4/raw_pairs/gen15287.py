from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 441, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "event_type",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
)
