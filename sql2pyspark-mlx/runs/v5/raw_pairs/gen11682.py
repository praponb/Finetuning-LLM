from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("event_type") != "click")

event_month_summary = (
    recent_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    event_month_summary
    .orderBy(F.col("duration_sec_range").desc(), F.col("pct_product").desc())
)
