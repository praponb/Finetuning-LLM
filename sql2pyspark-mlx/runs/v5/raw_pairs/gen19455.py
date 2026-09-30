from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 2)
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
