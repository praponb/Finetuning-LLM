from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & F.col("session_id").isin([6, 39, 40]))
)

agg = (
    filtered_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
)

result = (
    agg
    .select("event_month", "session_id", "cart_duration_sec")
    .orderBy("event_month", F.col("session_id").desc())
)
