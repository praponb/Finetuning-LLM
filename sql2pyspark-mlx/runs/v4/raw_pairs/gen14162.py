from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(~F.col("event_type").isin(["purchase", "click", "add_to_cart"]))
    .withColumn("page_title", F.initcap(F.trim("page")))
)

summary = (
    recent_web_events
    .groupBy("event_type")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    summary
    .select("event_type", "avg_duration_sec")
    .orderBy("event_type")
)
