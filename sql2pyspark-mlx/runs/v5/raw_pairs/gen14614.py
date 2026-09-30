from pyspark.sql import functions as F

src = df_web_events.filter(F.col("page") == "/checkout")

page_summary = (
    src
    .groupBy("page")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)

result = (
    page_summary
    .orderBy(F.col("page").desc(), F.col("unique_event_types").desc())
    .limit(10)
)
