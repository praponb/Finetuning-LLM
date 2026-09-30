from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([20, 39]))
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .filter(F.col("unique_sessions") >= 2)
    .orderBy(
        F.col("unique_sessions").desc(),
        F.col("cart_count").desc(),
        "event_quarter",
    )
    .limit(5)
)
