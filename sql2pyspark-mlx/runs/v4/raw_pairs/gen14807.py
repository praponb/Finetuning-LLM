from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
    .filter(F.col("pct_checkout") < 44)
    .orderBy(F.col("event_hour").desc())
)
