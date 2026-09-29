from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 16, 30]))
    .groupBy("page", "event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .filter(F.col("product_duration_sec") < 69)
    .orderBy("event_type")
)
