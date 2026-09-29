from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "click", "search"]) & F.col("event_type").isin(["purchase", "search", "click"]))
    .groupBy("page")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .orderBy(F.col("page").desc())
)
