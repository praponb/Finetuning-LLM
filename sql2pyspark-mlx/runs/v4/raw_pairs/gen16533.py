from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/search", "/home"]))
    .groupBy("event_type", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") >= 52)
    .orderBy("event_month")
)
