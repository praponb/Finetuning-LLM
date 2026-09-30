from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([20, 28]))
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
    .filter(F.col("duration_sec_range") > 80)
    .select("session_id", "duration_sec_range", "event_type_values")
    .orderBy("duration_sec_range")
)
