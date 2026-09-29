from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("duration_sec_range") >= 69)
    .select("session_id", "event_type_list", "duration_sec_range")
    .orderBy(F.col("session_id").desc())
)
