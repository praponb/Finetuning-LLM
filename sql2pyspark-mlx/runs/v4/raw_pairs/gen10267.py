from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.to_date("event_time") >= "2025-01-21") & (F.col("page") == "/home") & F.col("page").isin(["/home", "/product", "/cart"]))
    .withColumn("page_upper", F.upper("page"))
)

web_events_stats = (
    valid_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = web_events_stats.filter(F.col("duration_sec_range") > 51)
