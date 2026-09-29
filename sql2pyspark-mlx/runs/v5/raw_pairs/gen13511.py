from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "page_view"]))
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
)

summary = (
    src
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    summary
    .filter(F.col("unique_event_types") >= 6)
    .orderBy(F.col("pct_search").desc())
)
