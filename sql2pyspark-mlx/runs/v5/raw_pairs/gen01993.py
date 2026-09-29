from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
        F.grouping("event_type").alias("grouping_id"),
    )
    .orderBy(F.col("checkout_duration_sec").desc())
)
