from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("page") == "/checkout")

event_type_group_summary = (
    filtered_web_events
    .groupBy(
        F.when(F.col("event_type").isin(["page_view", "add_to_cart"]), "group_a").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    event_type_group_summary
    .filter(F.col("total_duration_sec") > 1400)
    .orderBy(F.col("total_duration_sec").desc())
)
