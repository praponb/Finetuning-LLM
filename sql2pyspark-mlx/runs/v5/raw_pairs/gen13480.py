from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 20) & (F.col("page").isin(["/search", "/product", "/checkout"]) | ~F.col("event_type").isin(["page_view", "purchase", "click"])))
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)
