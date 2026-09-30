from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("duration_sec").between(151, 185))
    .withColumn("page_length", F.length("page"))
)

event_type_summary = (
    recent_web_events
    .groupBy("event_type")
    .agg(
        F.sum("page_length").alias("total_page_length"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    event_type_summary
    .filter(F.col("total_page_length") > 230)
    .select("event_type", "total_page_length")
    .orderBy("event_type")
)
