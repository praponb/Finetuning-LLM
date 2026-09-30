from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("session_id").isin([2, 11, 16, 24]))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 2))
)

event_month_summary = (
    src
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
        F.count("duration_sec_rounded").alias("duration_sec_rounded_count"),
    )
)

result = (
    event_month_summary
    .filter(F.col("pct_checkout") >= 33)
    .select("event_month", "pct_checkout", "duration_sec_rounded_count")
    .orderBy(F.col("pct_checkout").desc())
)
