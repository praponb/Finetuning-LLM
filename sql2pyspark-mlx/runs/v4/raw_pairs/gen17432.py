from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.to_date("event_time") >= "2025-02-01") & F.col("event_type").isin(["click", "purchase", "page_view"])) | (F.to_date("event_time") >= "2024-05-11"))
    .groupBy("event_type")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .orderBy("event_type")
)
