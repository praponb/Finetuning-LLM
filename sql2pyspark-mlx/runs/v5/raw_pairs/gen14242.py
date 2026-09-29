from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("session_id").isin([11, 26]))
    .withColumn("page_title", F.initcap(F.trim("page")))
)

event_year_summary = (
    src
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = event_year_summary.select("event_year", "max_duration_sec")
