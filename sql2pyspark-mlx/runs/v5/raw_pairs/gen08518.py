from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/cart")))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.lower("event_type").alias("event_type_lower"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

result = (
    base
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
    )
    .filter(F.col("duration_sec_stddev") >= 0)
    .orderBy(F.col("event_type").desc())
)
